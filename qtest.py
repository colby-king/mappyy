from pytbls.connection import DBClient
from pytbls.tables import *
import datetime

TMS_DEV_CONNECT = (
	r'Driver={SQL Server};'
	r'Server=WINESSSSDEV11;'
	r'Database=tmsenterprise;'
	r'Trusted_Connection=yes;'
	r'MARS_Connection=Yes;'
)

def main():

	client = DBClient(TMS_DEV_CONNECT)

	definition = client.query_table_def('tblLocationCodes')

	#print(definition)

	#tblLocationCodes_def = TableDefinition(definition, 'tblLocationCodes')

	loc_data = {
	"IDSegment": 1,
	"IDSite": 180,
	"IDBuilding": 177,
	"Code": "123",
	"Description": "Location",
	"Show": 1,
	"IDAccount":None,
	"ShowInQuery": 1,
	"IDSpace": 0,
	"IsMeterLocation": 0,
	"TagNumber": None,
	#"DateCreated":datetime.datetime.now(),
	"DateUpdated": datetime.datetime.now()
	}

	#mr = MappyRow(tblLocationCodes_def, loc_data)
	#print(mr.sql_insert)

	tbl = MappyTable(None, definition, 'tblLocationCodes')


main()