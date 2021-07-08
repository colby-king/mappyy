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

def main():

	client = DBClient(TMS_DEV_CONNECT)

	loc_data = [
		{"IDSegment": 10,"IDSite": 9,"IDBuilding": 177,"Code": "abdz1","Description": "Location1","Show": 1,"IDAccount":None,"ShowInQuery": 1,"IDSpace": None,"IsMeterLocation": 0,"TagNumber": None,"DateCreated":datetime.datetime.now(),"DateUpdated": datetime.datetime.now()},
		{"IDSegment": 10,"IDSite": 9,"IDBuilding": 177,"Code": "abdz2","Description": "Location2","Show": 1,"IDAccount":None,"ShowInQuery": 1,"IDSpace": None,"IsMeterLocation": 0,"TagNumber": None,"DateCreated":datetime.datetime.now(),"DateUpdated": datetime.datetime.now()},
		{"IDSegment": 10,"IDSite": 9,"IDBuilding": 177,"Code": "abdz3","Description": "Location3","Show": 1,"IDAccount":None,"ShowInQuery": 1,"IDSpace": None,"IsMeterLocation": 0,"TagNumber": None,"DateCreated":datetime.datetime.now(),"DateUpdated": datetime.datetime.now()}
	]

	tblLocationCodes = client.get_table('tblLocationCodes')
	data_w_pks = tblLocationCodes.add_all(loc_data)
	print(data_w_pks)

	# test_data = [
	# 	{'ID': 1005901064, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901063, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901062, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901061, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901060, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901059, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901058, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901057, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901056, 'col1': 'data', 'col2': 3},
	# 	{'ID': 1005901055, 'col1': 'data', 'col2': 3}
	# ]

	# tblWorkOrders = client.get_table('tblWorkOrders')

	# tblWorkOrders.join(
	# 	test_data, 
	# 	select=['IDWorkOrder', 'WONumber', 'DateCreated'], 
	# 	on=('ID', 'IDWorkOrder')
	# )

main()