import pyodbc
from enum import Enum
from abc import ABC
import csv
import pytbls.exceptions
from pytbls.sql import * 
from pytbls.sql import __PY_TO_SQLTYPES
from tabulate import tabulate 
from collections import OrderedDict

class TableDefinition(object):

	def __init__(self, definition, tablename, schema=None, *args, create_col_defs=True):
		self._columns = {}
		self._name = tablename
		self._schema = schema
		# Initialize Column def objects if not provided 
		if create_col_defs:
			for col_def in definition:
				try:
					self._columns[col_def['name']] = ColumnDefinition(col_def)
				except KeyError:
					raise ValueError('Name is a required attribute for Column')
		else:
			self.columns = definition

		# Set primary key
		pks = [col for col in self.columns if col.is_pk]
		self._primary_key = pks

		# Set identity column 
		id_col = [col for col in self.columns if col.is_identity]
		if id_col:
			if len(id_col) > 1:
				raise pytbls.exceptions.IllegalTableDefinitionError('Table cannot have more than 1 identity column')
			self._identity = id_col[0]
		else:
			self._identity = None


	@property
	def schema(self):
		return self._schema
	
	@property
	def identity(self):
		return self._identity

	@property
	def composite_key(self):
		return len(self.primary_key) > 1
	
	

	@property
	def name(self):
		return self._name
	

	@property
	def primary_key(self):
		return self._primary_key

	@property
	def column_names(self):
		"""Returns a list of all column names"""
		return [col.name for col in self.columns]

	@property
	def pk_column_names(self):
		return [col.name for col in self.primary_key]
	

	@property
	def required_columns(self):
		"""Returns a list of non-nullable column objects"""
		return [col for col in self.columns if not col.is_nullable]

	@property
	def required_column_names(self):
		"""Returns a list of non-nullable column names"""
		return [col.name for col in self.columns if not col.is_nullable]

	@property
	def columns(self):
		return [col for name, col in self._columns.items()]

	def __iter__(self):
		return iter(self.columns)
	
class ColumnDefinition(object):

	def __init__(self, definition, **kwargs):
		definition.update(kwargs)
		self._definition = definition
		if type(definition) is dict or kwargs:
			try:
				self._name = definition.get('name') or kwargs.get('name')
				self._max_length = definition.get('max_length') or kwargs.get('max_length')
				self._scale = definition.get('scale') or kwargs.get('scale')
				self._is_nullable = definition.get('is_nullable') or kwargs.get('is_nullable')
				self._data_type = definition.get('type') or kwargs.get('type')
				self._column_id = definition.get('column_id') or kwargs.get('column_id')
				self._is_pk = definition.get('is_primary_key')
				self._is_identity = definition.get('is_identity')
				self._default_value = definition.get('default_value')
			except KeyError as e:
				raise ValueError('Required column attribute not set: {}'.format(e.args[0]))
		else:
			raise ValueError('Column attributes not provided to Column Definition object')

	@property
	def is_identity(self):
		return self._is_identity

	@property
	def default_value(self):
		return self._default_value
	
	

	@property
	def definition(self):
		return self._definition
	

	@property
	def is_pk(self):
		return self._is_pk
	

	@property
	def name(self):
		return self._name

	@property
	def max_length(self):
		return self._max_length

	@property
	def scale(self):
		return self._scale

	@property
	def is_nullable(self):
		return self._is_nullable

	@property
	def required(self):
		return self.is_nullable
	

	@property
	def data_type(self):
		return self._data_type

	@property
	def column_id(self):
		return self._column_id
	
	def __str__(self):
		return self.name

	def __repr__(self):
		return 'Column(name: {})'.format(self.name)

	def __eq__(self, other):
		if isinstance(other, ColumnDefinition):
			return other.name == self.name
		elif isinstance(other, str):
			print('Checking string equality on:', other)
			return other.lower() == self.name.lower()
		return False

	def __hash__(self):
		return hash(self.name)

class MappyTable(TableDefinition):

	def __init__(self, driver, tabledef, tablename, *args):
		super().__init__(tabledef, tablename, *args, create_col_defs=True)
		self.__driver = driver


	def get_import_csv(self, dest, required_only=True, include_index=False, *args):

		if not required_only:
			if len(args) > 0:
				raise ValueError("Cannot specify additional columns if required_only is False")
			headers = self.column_names
		else:
			headers = self.required_column_names
			if args:
				headers.append(args)

		if not include_index:
			headers.remove(self.primary_key.name)

		write_csv(dest, headers, None)

	def update(self, update_dict, pk=None):
		
		composite_key = type(pk) == list 
		pks, update_cols = self.__validate_update_by_pk(update_dict)
		sql_update = SQLBuilder.update_by_pk(self.name, list(update_cols.keys()), list(pks.keys()))
		write_vals = (list(update_cols.values()) + list(pks.values()))
		self.__driver.write(sql_update, *write_vals)


	def update_all(self, data_list):

		# validate the input and parse columns 
		pks, update_cols = self.__validate_update_by_pk(data_list[0])
		# make sure our data list is in tabular format
		validate_dl(data_list)
		# generate update statement
		sql_update = SQLBuilder.update_by_pk(self.name, list(update_cols.keys()), list(pks.keys()))

		print(pks)
		print(update_cols)
		print(sql_update)



	def _validate_cols(self, data_dict):

		for column in self.required_columns:
			if column not in list(data_dict.keys()):
				if not column.is_identity and not column.default_value:
					raise pytbls.exceptions.DataValidationError(
						"Attribute '{}' is required and is None".format(column.name)
					)


	def __validate_insert(self, data_dict, exact_match=False):
		"""Checks that all columns required for insert are present.
		   and returns a dict of insertable data (removes extra columns)
		"""

		# Check that every required column is present in data_dict 
		for column in self.required_columns:
			if column.name not in data_dict.keys():
				# Exclude identity and columns with defaults
				if not column.is_identity and not column.default_value:
					raise pytbls.exceptions.DataValidationError("Attribute '{}' is required and is None".format(column.name))

		# Parse out columns that can be inserted 
		if not exact_match:
			insertable = {}
			for col, val in data_dict.items():
				if col in self.column_names: 
					insertable[col] = val
			return insertable
		# Otherwise, check that they all can be inserted
		else:
			for col in data_dict.keys():
				if col not in self.columns:
					raise pytbls.exceptions.DataValidationError(
						"Cannot match '{}'' to column in {}. Fix the column name in the input or set exact_match to False".format(col, self.name)
					)
			return data_dict


	def __validate_update_by_pk(self, data_dict):
		"""Checks that each PK col is present in the update_dict"""

		pk_cols = OrderedDict()
		for pk_col in self.pk_column_names:
			if pk_col not in data_dict.keys():
				raise pytbls.exceptions.DataValidationError("Primary key '{}' is required for update".format(pk_col))
			pk_cols[pk_col] = data_dict[pk_col]

		update_cols = OrderedDict()
		for key, val in data_dict.items():
			if key not in self.column_names:
				raise pytbls.exceptions.DataValidationError("Attribute '{}' doesn't exist on {} and cannot be updated".format(self.name, key))
			
			if key not in self.pk_column_names:
				update_cols[key] = data_dict[key]

		if len(update_cols) > 0:
			return (pk_cols, update_cols)

		raise pytbls.exceptions.DataValidationError("No columns specified in the update dictionary")

	def _trim_data_dict(self, data_dict):
		insertable = {}
		for col, val in data_dict.items():
			if col in self.columns:
				insertable[col] = val 

		return insertable

	def add(self, data_dict, exact_match=True, **data):
		# update data_dict with extra kwargs 
		data_dict.update(data)
		# Separate column checking from the column matching
		self._validate_cols(data_dict)
		insertable = data_dict
		if not exact_match:
			insertable = self._trim_data_dict(data_dict)
		sql_insert = SQLBuilder.insert(self.name, insertable.keys())
		row_id = self.__driver.write(sql_insert, *insertable.values(), identity=True)

		if self.identity:
			insertable[self.identity.name] = row_id

		return insertable


	def add_all(self, data_list, exact_match=False, chunksize=None):
		"""
		Adds a data list to a table in the database

		Arguments:
		param data_list: a list of dictionaries in tabular format
		param exact_match: removes key/value pairs that can't be matched on 
						   the table definition if False. Assumes keys match
						   table column names if True
		param chunksize: specify # of records to commit as a time

		return new data_list with appended insert identities 
		"""
		
		# make sure data_list is in tabular format
		validate_dl(data_list)
		# peek to see we have all of the right columns 
		self._validate_cols(data_list[0])

		# if specifies extraneous columns, remove them
		if not exact_match:
			data_list = [self._trim_data_dict(d) for d in data_list]

		# create temporary table to hold ids
		sql_id_store_table = SQLBuilder.create_tmp_table([('OutputID', 'INT')])
		self.__driver.write(sql_id_store_table, commit=True)


		sql_insert = SQLBuilder.insert_and_output()
		self.__driver.executemany(sql_insert, data_list, fast=True)

		

		


	def join(self, data_list, select=None, on=(), join_type='INNER'):
		"""
		Joins a data list--a list of dictionaries--with a table in the 
		database. 
		
		Arguments
		param data_list: list of dictionaries, like a table 
		param on: the name of the column in the data list you want to join on
		param table_col: name of the column in the table you want to join the 
					     data_list on. If none, it's defaul will be set to on.
	    param: join_type: specifies the type of join operation you want to perform

	    return: new data_list with joined data 
		"""

		dl_col, table_col = on

		if dl_col not in data_list[0].keys():
			raise ValueError("Cannot join data. Data list does not have column '{}' in its keys".format(dl_col))
		elif table_col not in self.column_names:
			raise ValueError("Cannot join data. Table {} does not have column '{}' in its keys".format(self.name, table_col))

		select_list = []
		if not select:
			# select all fields in provided data and in DB Table if not specified
			select_list = ['*']
		else:
			# verify all fields in the select list are in this table
			for col in select:
				if col not in self.column_names:
					raise pytbls.exceptions.DataValidationError("{} is an invalid column name for table {}".format(col, self.name))
				full_colname = '{}.{}'.format(self.name, col)
				select_list.append(full_colname)

			# select dl columns too
			select_list = ['#Temporary.*'] + select_list

		if not on:
			raise TypeError("specify column in datalist and table to join: on=(dl_col, tbl_col)")


		# try to get data types from first item and hope the list is consistent
		# Add checks to validate table later 
		columns = get_dl_columns(data_list[0])
		
		# Create the temporary table
		sql_create_table = SQLBuilder.create_tmp_table(columns)
		self.__driver.write(sql_create_table, commit=True)

		# Insert dl list into tmp DB table
		col_names = [col[0] for col in columns]
		sql_insert = SQLBuilder.insert('#Temporary', col_names)
		self.__driver.executemany(sql_insert, data_list, fast=True)
		self.__driver.commit()


		query = SQLBuilder.build_query(
			'#Temporary',
			select_list=select_list,
			table_joins=[(self.name, dl_col, table_col, join_type)]
		)

		data = self.__driver.read(query, to_dict=True)

		return data



	def test_data(self, data_dict, **data):
		pass

		

	def print_info(self):
		data = [col.definition for col in self.columns]
		headers = list(data[0].keys())
		data = [col.values() for col in data]
		print(tabulate(data, headers=headers))
	


	

########### data list functions ###############

def get_dl_as_tuples(data_list):
	""" Takes a list of dictories and returns a list of tuples holding the dicts values"""

	return [tup(d.values()) for d in data_list]



def validate_dl(data_list):
	"""Validates that data lists are in a tabular format"""
	try:
		cols = data_list[0].keys()
	except KeyError:
		raise ValueError('data list must contain a list of at least one dictionary to be considered valid')

	for i, row in enumerate(data_list):
		if row.keys() != cols:
			raise ValueError('dictionary at position {} has a unique set of keys.'.format(i))




def get_dl_columns(data_row):
	"""
	returns a list of tuples containing the corresponding SQL type and column name
	
	Arguments
	param data_row: a dictionary that represents a row of data
	return: list of tuples containing the dict entry's key and corresponding 
		    SQL data type
	"""
	columns = []
	for key, tup in data_row.items():
		try:
			columns.append((
				key,
				__PY_TO_SQLTYPES[type(tup)]
			))
		except KeyError:
			raise pytbls.exceptions.TypeNotSupportedError("type {} not supported by mappy".format(type(tup)))

	return columns
	


def convert_dl_col_type(data_list, col_name, py_type, col_index=None, default_on_fail=False, default=None):

	import ast
	from dateutil import parser

	eval_functions  = {
		int: int,
		str: str,
		float: float,
		bool: bool,
		datetime.datetime: parser.parse
	}

	if py_type not in eval_functions.keys():
		raise TypeError('cannot convert column {} to type {}. Type not supported'.format(col_name, str(py_type)))


	for i, row in enumerate(data_list):
		convert_type = eval_functions[py_type]
		try:
			row[col_name] = convert_type(row[col_name])
		except ValueError as e:
			if use_default_on_fail:
				row[col_name] = default
			else:
				raise ValueError('Unable to convert to type {} at column {} at row {}'.format(str(py_type), col_name, str(i)))




def write_csv(filename, headers, data):
	with open(filename, 'w', newline='') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=headers)
		writer.writeheader()
		if data:
			for row in data:
				writer.writerow(row)

def read_csv(filename, read_empty_as_none=True, strip=True):
	data = []
	with open(filename) as f:
		data_reader = csv.DictReader(f)
		for row in data_reader:
			if read_empty_as_none:
				row = {k: read_blank_as if v == '' else v for k, v in row.items()}
			data.append(dict(row))

	return data

