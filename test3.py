from pytbls.connection import DBClient
from pytbls.tables import *
import datetime

TMS_DEV_CONNECT = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=swtms0db5001;'
	r'Database=tmsenterprise_TST;'
	r'Trusted_Connection=yes;'
	r'MARS_Connection=Yes;'
)

TMS_PRD_CONNECT = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=swtms0db1001;'
	r'Database=tmsenterprise_TST;'
	r'Trusted_Connection=yes;'
	r'MARS_Connection=Yes;'
)

def main():

	client = DBClient(TMS_DEV_CONNECT)
	update_test = {
	'IDResource': 72,
	'ResourceNumber': 'JSHELLO',
	'FirstName': 'JOSEPHalkdsjf',
	'LastName': 'SCIULLIasdf'
	}
	tblResources = client.get_table('tblResources')
	tblResources.update(update_test)



main()