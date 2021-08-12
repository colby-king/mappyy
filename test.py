import csv
from pytbls.tables import *
from pytbls.connection import DBClient

TMS_PRD_CONNECT = (
	r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=swtms0db1001;'
	r'Database=tmsenterprise;'
	r'Trusted_Connection=yes;'
	r'MARS_Connection=Yes;'
)

def main():

	data = read_csv('test_data/iphones_7_12_21.csv')

	#convert_dl_col_type(data, 'Phone Number', int)

	for row in data:
		print(row)

	validate_dl(data)

	client = DBClient(TMS_PRD_CONNECT)
	tblResources = client.get_table('tblResources')

	new_data = tblResources.join(
		data, 
		select=['UserField9', 'UserField8', 'UserField7', 'UserField6', 'UserField5', 'IDResource', 'IDSegment', 'ResourceNumber', 'FirstName', 'LastName', 'IDStatus'], 
		on=('PagerEmail', 'PagerEmail'),
		join_type='LEFT'
	)

	write_csv('joined_iphones.csv', list(new_data[0].keys()), new_data)

	#print(new_data)



def read_csv(filename, read_empty_as_none=True, strip=True):
	data = []
	with open(filename) as f:
		data_reader = csv.DictReader(f)
		for row in data_reader:
			data.append(dict(row))

	return data


main()