




main():

	my_data = [
		{'ID': 1, 'col1': 'data', 'col2': 'data', 'col3': 'data'}
		{'ID': 2, 'col1': 'data', 'col2': 'data', 'col3': 'data'}
		{'ID': 3, 'col1': 'data', 'col2': 'data', 'col3': 'data'}
	]

	row = {'ID': 2, 'col1': 'data', 'col2': 'data', 'col3': 'data'}



	
	client = DBClient(cnxn_str)

	row = client.insert('table', row)


	inserted = client.insert_all('table', my_data)

	client.update('table', row)
	client.update_all('table', my_data)

	client.delete('table', row)
	client.delete_all('table', my_data)

	db_data = client.query("select * from table")

	client.join('table', my_data, on=('ID', 'IDCol'), kind='INNER')







main()


# CREATE TABLE MyTable
# (
#     MyPK INT IDENTITY(1,1) NOT NULL,
#     MyColumn NVARCHAR(1000)
# )

# DECLARE @myNewPKTable TABLE (myNewPK INT)

# INSERT INTO 
#     MyTable
# (
#     MyColumn
# )
# OUTPUT INSERTED.MyPK INTO @myNewPKTable
# SELECT
#     sysobjects.name
# FROM
#     sysobjects

# SELECT * FROM @myNewPKTable