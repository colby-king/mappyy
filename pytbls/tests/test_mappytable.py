import pytest
from unittest.mock import Mock
from pytbls.tables import *
from pytbls.connection import Driver

class TestMappyTable:

	@pytest.fixture(scope='module', autouse=True)
	def table_def(self):
		table_def = [
			{'name': 'AddressID', 'max_length': 4, 'scale': 0, 'is_nullable': False, 'data_type': 'int', 'column_id': 1, 'is_primary_key': True, 'is_identity': True, 'default_value':None},
			{'name': 'AddressLine1', 'max_length': 120, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 2, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'AddressLine2', 'max_length': 120, 'scale': 0, 'is_nullable': True, 'data_type': 'nvarchar', 'column_id': 3, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'City', 'max_length': 60, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 4, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'StateProvinceID', 'max_length': 4, 'scale': 0, 'is_nullable': False, 'data_type': 'int', 'column_id': 5, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'PostalCode', 'max_length': 30, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 6, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'rowguid', 'max_length': 16, 'scale': 0, 'is_nullable': False, 'data_type': 'uniqueidentifier', 'column_id': 7, 'is_primary_key': False, 'is_identity': False, 'default_value':None},
			{'name': 'ModifiedDate', 'max_length': 8, 'scale': 3, 'is_nullable': False, 'data_type': 'datetime', 'column_id': 8, 'is_primary_key': False, 'is_identity': False, 'default_value':None}
		]
		return table_def

	@pytest.fixture(scope='module', autouse=True)
	def data(self):
		data = {
			'AddressID': '',
			'AddressLine1': '1971 Napa Ct.',
			'AddressLine2': '',
			'City': 'Pittsburgh',
			'StateProvinceID': '19',
			'PostalCode': '15123',
			'rowguid': 'febf8191-9804-44c8-877a-33fde94f0075',
			'ModifiedDate': '2008-12-17 00:00:00.000'
		}
		return data

	@pytest.fixture(scope='module', autouse=True)
	def data_inconsistent_case(self):
		data = {
			'addressID': '',
			'AddressLine1': '1971 Napa Ct.',
			'addressline2': '',
			'city': 'Pittsburgh',
			'StateProvinceID': '19',
			'PostalCode': '15123',
			'rowguid': 'febf8191-9804-44c8-877a-33fde94f0075',
			'ModifiedDate': '2008-12-17 00:00:00.000'
		}
		return data

	@pytest.fixture(scope='module', autouse=True)
	def tablename(self):
		return 'Address'


	@pytest.fixture(scope='module', autouse=True)
	def mappyytable(self, table_def, tablename):

		driver = Mock()
		driver.write.return_value = 3
		return MappyTable(driver, table_def, tablename)


	def test_column_validation(self, mappyytable, data):

		mappyytable._validate_cols(data)

	def test_column_validation_inconsistent(self, mappyytable, data_inconsistent_case, data):

		mappyytable._validate_cols(data_inconsistent_case)

	def test_add(self, mappyytable, data):
		
		r = mappyytable.add(data)
		assert r == {
			'AddressID': 3,
			'AddressLine1': '1971 Napa Ct.',
			'AddressLine2': '',
			'City': 'Pittsburgh',
			'StateProvinceID': '19',
			'PostalCode': '15123',
			'rowguid': 'febf8191-9804-44c8-877a-33fde94f0075',
			'ModifiedDate': '2008-12-17 00:00:00.000'
		}

		




















