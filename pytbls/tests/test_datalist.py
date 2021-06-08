import pytest
import mock 
from pytest_mock import mocker 
from pytbls.tables import (
	validate_dl,
	get_dl_columns,
	convert_dl_col_type
)


class TestDataList:

	@pytest.fixture(scope='module', autouse=True)
	def data(self):
		data = [
			{'ID': 1, 'col1': '2021-06-08 14:36:43.889928', 'col2': '0', 'col3': '123998'},
			{'ID': 2, 'col1': '2021-06-08 14:36:43.88', 'col2': '1', 'col3': '123998'},
			{'ID': 3, 'col1': '2021-06-08 14:36:43.889', 'col2': '1', 'col3': '123998'},
			{'ID': 4, 'col1': '2021-06-08 14:36:43', 'col2': '0', 'col3': '123998'},
			{'ID': 5, 'col1': '2021-06-08 14:36:43', 'col2': '0', 'col3': 123998},
			{'ID': 6, 'col1': '2021-06-08 14:36:43', 'col2': '1', 'col3': '123998'},
			{'ID': 7, 'col1': '2021-06-08 14:36:43', 'col2': '1', 'col3': '123998'},
			{'ID': 8, 'col1': '2021-06-08 14:36:43', 'col2': 0, 'col3': '123998'},
			{'ID': 9, 'col1': '2021-06-08', 'col2': '1', 'col3': '123998'},
		]

		return data

	@pytest.fixture(scope='module', autouse=True)
	def bad_data(self):
		data = [
			{'ID': 1, 'col1': '2021-06-08 14:36:43.889928', 'col2': '0', 'col3': '123998'},
			{'ID': 2, 'col2': '1', 'col3': '123998'},
			{'ID': 3, 'col1': '2021-06-08 14:36:43.889', 'col2': '1', 'col3': '123998'},
			{'ID': 4, 'col1': '2021-06-08 14:36:43', 'col2': '0', 'col3': '123998'},
			{'ID': 5, 'col1': '2021-06-08 14:36:43', 'col2': '0', 'col3': 123998},
			{'ID': 6, 'col1': '2021-06-08 14:36:43', 'col2': '1', 'col3': '123998'},
			{'ID': 7, 'col1': '2021-06-08 14:36:43', 'col3': '123998'},
			{'ID': 8, 'col1': '2021-06-08 14:36:43', 'col2': 0, 'col3': '123998'},
			{'ID': 9, 'col1': '2021-06-08', 'col2': '1', 'col3': '123998'},
			{'ID': 10,'col1': '2021-06-08', 'col2': '1', 'col3': '1239a98'},
		]

		return data

	def test_validate_dl_empty_data_list(self, data):
		# Check this test
		empty = {}
		with pytest.raises(ValueError) as e_info:
			validate_dl(empty)

	def test_validate_dl_good_data_list(self, data):
		validate_dl(data)


	def test_validate_dl_bad_data_list(self, bad_data):
		with pytest.raises(ValueError) as e_info:
			validate_dl(bad_data)

	def test_dl_col_conversion_int(self, data):

		COL_TO_CONVERT = 'col3'
		NEW_TYPE = int

		convert_dl_col_type(data, COL_TO_CONVERT, int)

		for row in data:
			if type(row[COL_TO_CONVERT]) != NEW_TYPE:
				assert False

	def test_dl_col_conversion_bad_int(self, bad_data):

		COL_TO_CONVERT = 'col3'
		NEW_TYPE = int

		with pytest.raises(ValueError) as e_info:
			# Should fail on ID: 10
			convert_dl_col_type(bad_data, COL_TO_CONVERT, int)


