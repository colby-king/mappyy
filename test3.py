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
	r'Database=tmsenterprise;'
	r'Trusted_Connection=yes;'
	r'MARS_Connection=Yes;'
)

def main():

	dev_client = DBClient(TMS_DEV_CONNECT)

	prd_client = DBClient(TMS_PRD_CONNECT)

	r_data = prd_client.query("SELECT IDResource, ResourceNumber FROM tblResources WHERE IDStatus = 10")
	#dev_client.write_csv('ResourceData.csv', r_data)

	update_ct = 0

	dev_tblResources = dev_client.get_table('tblResources')
	for line in r_data:
		dev_tblResources.update(line)
		update_ct += 1

	print('Total row updates: {}'.format(update_ct))




main()