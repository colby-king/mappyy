import pytbls.exceptions
import datetime
import uuid


class SQLServerSystem(object):

	@staticmethod
	def get_column_info(table, schema=None):
		"""Query uses SQL server sys columns to gather information about a tables columns"""

		if schema:
			table = '{}.{}'.format(table, schema)

		sql = """
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
	               c.system_type_id
			  FROM sys.columns c
			  JOIN sys.types   t
			    ON c.system_type_id = t.user_type_id
			  CROSS APPLY (SELECT MAX(CASE WHEN ind.is_primary_key = 1 THEN 1 ELSE 0 END) AS is_primary_key FROM sys.index_columns ic
						   LEFT JOIN sys.indexes ind on ind.object_id = ic.object_id AND ind.index_id = ic.index_id
						   WHERE c.object_id = ic.object_id AND c.column_id = ic.column_id) AS ind
			 WHERE c.object_id    = Object_id(?)
		"""



class SQLBuilder(object):

	# Note avoid sql injection, validate table names. 
	# Also need to a way to validate column names when creating tmp tables


	@staticmethod
	def build_query(table, select_list=[], table_joins=[]):
		sl_len = len(select_list)
		sql = ('SELECT ' + '{}, ' * (sl_len - 1) + '{} ').format(*select_list)
		sql += 'FROM {} '.format(table)
		for join in table_joins:
			sql += '{} JOIN {} ON {}.{} = {}.{}'.format(join[3], join[0], join[0], join[1], table, join[2])

		return sql


	@staticmethod
	def create_tmp_table(column_list):
		"""
		returns sql that creates a temporary table given a list 
		of tuples of column names and data types 
		"""

		sql = 'CREATE TABLE #Temporary (\n'
		for col in column_list:
			if col != column_list[-1]:
				sql += '[{}] {},\n'.format(*col)
			else:
				sql += '[{}] {}\n)'.format(*col)
		return sql

	@staticmethod
	def create_tmp_table_id_store(column_list):
		"""
		returns sql that creates a temporary table given a list 
		of tuples of column names and data types 
		"""

		sql = 'CREATE TABLE #Temporary (\n'
		for col in column_list:
			sql += '[{}] {},\n'.format(*col)

		sql += '[{}] {}\n)'.format('insert_id', 'INTEGER')
		return sql

		
	@staticmethod
	def insert(tablename, columns, output_tablename=None, output_columns=[]):

		if output_tablename and len(output_columns) < 1:
			raise TypeError('No output columns specified.')


		#Build SQL string
		num_columns = len(columns)
		sql = 'INSERT INTO {} ('.format(tablename)
		sql += ('[{}], ' * (num_columns - 1) + '[{}])\n').format(*columns)
		if output_tablename:
			outline = (('INSERTED.{}, '*(len(output_columns) - 1)) + 'INSERTED.{}').format(*output_columns)
			sql += 'OUTPUT {} INTO {}\n'.format(outline, output_tablename)
		sql += 'VALUES ('
		sql += ('?, ' * (num_columns - 1) + '?)')
		return sql


	@staticmethod
	def insert_and_output(table, columns, output_table, pk):
		sql = self.insert(table, columns)
		sql += ('OUTPUT' + ('INSERTED.{},'*len(columns - 1)) + 'INSERTED.{}').format(*columns, pk)
		return sql


	@staticmethod
	def update_by_pk(tablename, update_cols, pk):
		if not tablename or not update_cols or not pk:
			raise ValueError('arguments must not be None or empty')

		num_updates = len(update_cols)
		sql = 'UPDATE {}\n'.format(tablename)
		sql += ('SET ' + '{} = ?,' * (num_updates - 1) + ' {} = ?\n').format(*update_cols)
		if type(pk) is list:
			pk_len = len(pk)
			sql += ('WHERE ' + '{} = ? AND ' * (pk_len - 1) + '{} = ?').format(*pk) 
		else:
			sql += 'WHERE {} = ?'.format(pk)
		return sql

	@staticmethod
	def delete_by_pk(tablename, pk):
		if not tablename or not pk:
			raise ValueError('arguments must not be None or empty')

		sql = 'DELETE FROM {}'.format(tablename)
		if type(pk) is list:
			pk_len = len(pk)
			sql += (' WHERE ' + '{} = ? AND ' * (pk_len - 1) + '{} = ?').format(*pk) 
		else:
			sql += ' WHERE {} = ?'.format(pk)
		return sql


__PY_TO_SQLTYPES = {
	
	None: 'NULL',
	bool: 'BIT',
	int: 'INTEGER',
	float: 'DOUBLE PRECISION',
	str: 'VARCHAR(MAX)',
	datetime.date: 'DATE',
	datetime.time: 'TIME',
	datetime.datetime: 'TIMESTAMP',
	uuid.UUID: 'GUID'
}