


class SQLBuilder(object):

	@staticmethod
	def insert(tablename, columns):
		if not tablename or not columns:
			raise ValueError('arguments must not be None or empty')

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
