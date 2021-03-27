from pytbls.connection import DBClient
from pytbls.tables import *
import datetime

def main():

	cnxn_str = 'DSN=MYMSSQL;UID=sa;PWD=reallyStrongPwd123;DATABASE=AdventureWorks2017;'
	client = DBClient(cnxn_str)

	tblPerson = client.get_table('Person', schema='Person')

	to_insert = {
		'BusinessEntityID': 101010,
		'PersonType': 'EM',
		'NameStyle': '0',
		'Title': 'Mr.',
		'FirstName': 'Colby',
		'MiddleName': 'Michael',
		'LastName': 'King',
		'Suffix': None,
		'EmailPromotion': 1,
		'AdditionalContactInfo': None,
		'Demographics': None
	}

	tblPerson.print_info()
	rv = tblPerson.add(to_insert)

	#tblPerson.print_info()
	#print(definition)

	#tblLocationCodes_def = TableDefinition(definition, 'tblLocationCodes')

# SELECT TOP (1000) [BusinessEntityID]
#       ,[PersonType]
#       ,[NameStyle]
#       ,[Title]
#       ,[FirstName]
#       ,[MiddleName]
#       ,[LastName]
#       ,[Suffix]
#       ,[EmailPromotion]
#       ,[AdditionalContactInfo]
#       ,[Demographics]
#       ,[rowguid]
#       ,[ModifiedDate]
#   FROM [AdventureWorks2017].[Person].[Person]

	


	#new_row = tbl.add(loc_data)
	#print(new_row.data_row)


def standard_usage():

	client = DBClient(cnxn_str)
	MyTable = client.get_table('tablename')
	MyTable.add({

	})


main()