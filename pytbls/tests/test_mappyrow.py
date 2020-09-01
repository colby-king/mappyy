import pytest

from pytbls.tables import *

class TestMappyRow:

	@pytest.fixture(scope='module', autouse=True)
	def table_def(self):
		table_def = [
			{'name': 'AddressID', 'max_length': 4, 'precision': 10, 'scale': 0, 'is_nullable': False, 'data_type': 'int', 'column_id': 1, 'is_primary_key': True},
			{'name': 'AddressLine1', 'max_length': 120, 'precision': 0, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 2, 'is_primary_key': False},
			{'name': 'AddressLine2', 'max_length': 120, 'precision': 0, 'scale': 0, 'is_nullable': True, 'data_type': 'nvarchar', 'column_id': 3, 'is_primary_key': False},
			{'name': 'City', 'max_length': 60, 'precision': 0, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 4, 'is_primary_key': False},
			{'name': 'StateProvinceID', 'max_length': 4, 'precision': 10, 'scale': 0, 'is_nullable': False, 'data_type': 'int', 'column_id': 5, 'is_primary_key': False},
			{'name': 'PostalCode', 'max_length': 30, 'precision': 0, 'scale': 0, 'is_nullable': False, 'data_type': 'nvarchar', 'column_id': 6, 'is_primary_key': False},
			{'name': 'SpatialLocation', 'max_length': -1, 'precision': 0, 'scale': 0, 'is_nullable': True, 'data_type': 'geography', 'column_id': 7, 'is_primary_key': False},
			{'name': 'rowguid', 'max_length': 16, 'precision': 0, 'scale': 0, 'is_nullable': False, 'data_type': 'uniqueidentifier', 'column_id': 8, 'is_primary_key': False},
			{'name': 'ModifiedDate', 'max_length': 8, 'precision': 23, 'scale': 3, 'is_nullable': False, 'data_type': 'datetime', 'column_id': 9, 'is_primary_key': False}
		]
		return table_def

	@pytest.fixture(scope='function', autouse=True)
	def data(self):
		data = {
			'AddressID': '',
			'AddressLine1': '1971 Napa Ct.',
			'AddressLine2': '',
			'City': 'Pittsburgh',
			'StateProvinceID': '19',
			'PostalCode': '15123',
			'SpatialLocation': '0xE6100000010CE0B4E50458DA47402F12A5F80C975EC0',
			'rowguid': 'febf8191-9804-44c8-877a-33fde94f0075',
			'ModifiedDate': '2008-12-17 00:00:00.000'
		}
		return data

	@pytest.fixture(scope='function', autouse=True)
	def mappy_row(self, mocker, data, table_def):
		td = TableDefinition(table_def, 'people')
		return MappyRow(td, data)


	def test_row_values(self, table_def, data, mappy_row):
		actual = list(data.values())
		assert mappy_row.values == actual

	def test_row_column_names(self, table_def, data, mappy_row):
		actual = list(data.keys())
		assert mappy_row.column_names == actual



		

