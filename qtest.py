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
	"IDSegment": 4,
	"IDSite": 9,
	"IDBuilding": 177,
	"Code": "rddizdd",
	"Description": "Location",
	"Show": 1,
	"IDAccount":None,
	"ShowInQuery": 1,
	"IDSpace": None,
	"IsMeterLocation": 0,
	"TagNumber": None,
	"DateCreated":datetime.datetime.now(),
	"DateUpdated": datetime.datetime.now()
	}

	#mr = MappyRow(tblLocationCodes_def, loc_data)
	#print(mr.sql_insert)

	tbl = MappyTable(client.driver, definition, 'tblLocationCodes')
	new_row = tbl.add(loc_data)
	print(new_row.data_row)

main()