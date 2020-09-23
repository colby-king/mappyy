import pyodbc
from pytbls.tables import MappyTable

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

	def write(self, sql, *args, commit=True):
		cursor = self.cnxn.cursor()
		cursor.execute(sql, *args)
		last_id = cursor.execute('SELECT @@IDENTITY').fetchone()
		if commit:
			self.commit()
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


	def get_table(self, tablename):

		cursor = self.driver.cnxn.cursor()

		table = cursor.tables(table=tablename).fetchone()
		if not table:
			raise ValueError('Table {} does not exist')

		primary_keys = cursor.primaryKeys(tablename).fetchall()
		pk_names = [pk.column_name for pk in primary_keys]
		columns = cursor.columns(tablename)
		table_def = []
		for col in columns:
			table_def.append({
				'name': col.column_name,
				'precision': col.column_size,
				'scale': col.decimal_digits,
				'is_nullable': bool(col.nullable),
				'data_type': col.type_name,
				'column_id': col.ordinal_position,
				'max_length': col.buffer_length,
				'is_primary_key': True if col.column_name in pk_names else False
			})

		return MappyTable(self.driver, table_def, tablename)



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
				   CASE WHEN ind.is_primary_key = 1 THEN 1 ELSE 0 END AS is_primary_key
			  FROM sys.columns c
			  JOIN sys.types   t
			    ON c.user_type_id = t.user_type_id
			  CROSS APPLY (SELECT MAX(CASE WHEN ind.is_primary_key = 1 THEN 1 ELSE 0 END) AS is_primary_key FROM sys.index_columns ic
						   LEFT JOIN sys.indexes ind on ind.object_id = ic.object_id AND ind.index_id = ic.index_id
						   WHERE c.object_id = ic.object_id AND c.column_id = ic.column_id) AS ind
			 WHERE c.object_id    = Object_id(?)
		"""
		# Query Data
		table_def = self.driver.read(qry_column_info, tablename, to_dict=True)
		return table_def



