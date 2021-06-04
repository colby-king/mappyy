import pyodbc
from pytbls.tables import MappyTable
#from pytbls.io import *
import csv

class Driver(object):

	def __init__(self, cnxn_str):
		self.cnxn_str = cnxn_str
		self.cnxn = pyodbc.connect(cnxn_str, autocommit=False)


	def _results_to_dict(self, data, cursor):
		cols = [col[0] for col in cursor.description]
		data_dict = []
		for row in data:
			data_dict.append(dict(zip(cols, row)))
		return data_dict

	def read(self, sql, *args, to_dict=False, fetchone=False):
		cursor = self.cnxn.cursor()
		data = cursor.execute(sql, *args).fetchall()
		self.commit()
		if to_dict:
			return self._results_to_dict(data, cursor)
		return data

	def write(self, sql, *args, commit=True, identity=False):
		"""Writes a record to the connection"""
		
		cursor = self.cnxn.cursor()
		cursor.execute(sql, *args)
		if commit:
			self.commit()
		if identity:
			last_id = cursor.execute('SELECT @@IDENTITY').fetchone()
			return int(last_id[0])

	def commit(self):
		"""Commits a transaction on the current connection"""
		self.cnxn.commit()

	def rollback(self):
		"""rolls back a transaction on the current connection"""
		self.cnxn.rollback()


class DBClient(object):

	def __init__(self, cnxn_str):

		if not cnxn_str:
			raise ValueError("Connection string must not be blank or None")

		self.driver = Driver(cnxn_str)
		self.__set_db_name()


	def __set_db_name(self):
		self.db_name = self.driver.read("SELECT DB_NAME();");


	def query(self, sql, *args):
		"""Executes a query on the current connection"""

		return self.driver.read(sql, *args, to_dict=True)

	def write_csv(self, filename, data):
		fieldnames = data[0].keys()

		with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
			dw = csv.DictWriter(csvfile, fieldnames=fieldnames)
			dw.writeheader()
			dw.writerows(data)



	def get_table(self, tablename, schema=None):
		"""returns a Mappy table object"""

		if schema:
			tablename = '{}.{}'.format(tablename, schema)
		# handle table not found
		table_def = self.__query_table_def(tablename)
		
		for col in table_def:
			print(col)

		return MappyTable(self.driver, table_def, tablename, schema)



	# def get_table(self, tablename, schema=None):
	# 	"""returns a Mappy table object"""

	# 	cursor = self.driver.cnxn.cursor()

	# 	table = cursor.tables(table=tablename).fetchone()
	# 	if not table:
	# 		raise ValueError("Table '{}' does not exist".format(tablename))

	# 	primary_keys = cursor.primaryKeys(tablename, schema=schema).fetchall()

	# 	pk_names = [pk.column_name for pk in primary_keys]
	# 	print('Primary key names', pk_names)
	# 	columns = cursor.columns(tablename)
	# 	table_def = []
	# 	for col in columns:
	# 		print(col)
	# 		table_def.append({
	# 			'name': col.column_name,
	# 			'precision': col.column_size,
	# 			'scale': col.decimal_digits,
	# 			'is_nullable': bool(col.nullable),
	# 			'data_type': col.type_name,
	# 			'column_id': col.ordinal_position,
	# 			'max_length': col.buffer_length,
	# 			'is_primary_key': True if col.column_name in pk_names else False
	# 		})

	# 	cols = cursor.foreignKeys(tablename).fetchall()
	# 	print(cols)
	# 	for col in cols:
	# 		print(col)

	# 	return MappyTable(self.driver, table_def, tablename, schema)



	def __query_table_def(self, tablename):
		"""Executres """
		qry_column_info = """
			SELECT c.name,
			       c.max_length,
			       c.precision,
			       c.scale,
			       c.is_nullable,
			       t.name [data_type],
				   c.object_id,
				   c.column_id,
				   CASE WHEN ind.is_primary_key = 1 THEN 1 ELSE 0 END AS is_primary_key,
	               c.is_identity,
	               c.system_type_id,
	               object_definition(c.default_object_id) [default_value]
			  FROM sys.columns c
			  JOIN sys.types   t
			    ON c.system_type_id = t.user_type_id
			  CROSS APPLY (SELECT MAX(CASE WHEN ind.is_primary_key = 1 THEN 1 ELSE 0 END) AS is_primary_key FROM sys.index_columns ic
						   LEFT JOIN sys.indexes ind on ind.object_id = ic.object_id AND ind.index_id = ic.index_id
						   WHERE c.object_id = ic.object_id AND c.column_id = ic.column_id) AS ind
			 WHERE c.object_id    = Object_id(?)
		"""
		# Query Data
		table_def = self.driver.read(qry_column_info, tablename, to_dict=True)
		return table_def



