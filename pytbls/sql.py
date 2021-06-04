import pytbls.exceptions


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



	@staticmethod
	def create_table(tablename, columns, data):
		pass

	@staticmethod
	def get_sql_type(t):
		try:
			return __PY_TO_SQLTYPES[t]
		except KeyError:
			raise pytbls.exceptions.TypeNotSupportedError("type not supported")

	@staticmethod
	def insert(tablename, columns):

		#Build SQL string
		num_columns = len(columns)
		sql = 'INSERT INTO {} ('.format(tablename)
		sql += ('{}, ' * (num_columns - 1) + '{})\n').format(*columns)
		sql += 'VALUES ('
		sql += ('?, ' * (num_columns - 1) + '?)')
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