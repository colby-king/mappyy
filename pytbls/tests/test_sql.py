import pytest
from pytbls.sql import SQLBuilder

class TestSQLBuilder:

	@pytest.fixture(scope='module', autouse=True)
	def data(self):
		data = {
			'AddressLine': '1971 Napa Ct.',
			'City': 'Pittsburgh',
			'State': 'PA',
			'PostalCode': '15123',
		}
		return data

	@pytest.fixture(scope='module', autouse=True)
	def tablename(self):
		return 'Address'


	def test_insert(self, tablename, data):
		expected = """INSERT INTO Address ([AddressLine], [City], [State], [PostalCode])\nVALUES (?, ?, ?, ?)"""
		actual = SQLBuilder.insert(tablename, data)
		assert expected == actual

	def test_insert_with_single_output(self, tablename, data):
		expected = """INSERT INTO Address ([AddressLine], [City], [State], [PostalCode])\nOUTPUT INSERTED.City INTO #tmpTable\nVALUES (?, ?, ?, ?)"""
		actual = SQLBuilder.insert(tablename, data, output_tablename='#tmpTable', output_columns=['City'])
		assert expected == actual

	def test_insert_with_multiple_output(self, tablename, data):
		expected = """INSERT INTO Address ([AddressLine], [City], [State], [PostalCode])\nOUTPUT INSERTED.City, INSERTED.State, INSERTED.PostalCode INTO #tmpTable\nVALUES (?, ?, ?, ?)"""
		actual = SQLBuilder.insert(tablename, data, output_tablename='#tmpTable', output_columns=['City', 'State', 'PostalCode'])
		print(actual)
		assert expected == actual

	def test_update(self, tablename, data):
		expected = """UPDATE Address\nSET AddressLine = ?, State = ?\nWHERE AddressID = ?"""
		actual = SQLBuilder.update_by_pk(tablename, ['AddressLine', 'State'], 'AddressID')
		assert expected == actual

	def test_delete(self, tablename):
		expected = """DELETE FROM Address WHERE AddressID = ?"""
		actual = SQLBuilder.delete_by_pk(tablename, 'AddressID')
		assert expected == actual

	def test_delete_composite_pk(self, tablename):
		expected = """DELETE FROM Address WHERE AddressID = ? AND PostalCode = ?"""
		actual = SQLBuilder.delete_by_pk(tablename, ['AddressID', 'PostalCode'])
		assert expected == actual

	def test_build_query(self, tablename):
		fields = ['field1', 'field2', 'field3']
		actual = SQLBuilder.build_query(
			'#temporary', 
			select_list=fields,
			table_joins=[('SampleTable', 'IDSample', 'IDTemp')]
		)
		expected = """SELECT [{}], [{}], [{}] FROM #temporary JOIN SampleTable ON SampleTable.IDSample = #temporary.IDTemp""".format(*fields)

		assert actual == expected



		

