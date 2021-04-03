

# Mappyy


## Usage 

### Connecting with Mappyy
```python
# Create a connection string and pass it to the DBClient object
cnxn_str = 'DSN=MYMSSQL;UID=sa;PWD=reallyStrongPwd123;DATABASE=AdventureWorks2017;'
client = DBClient(cnxn_str)

```

### Inserting Data

A single record can be inserted into a table:

```python
client = DBClient(cnxn_str)
tbl_person = client.get_table('Person', schema='Person')
new_person = tbl_person.add({
	'IDPerson': 1008,
	'PersonType': 'EM',
	'Title': 'Mr.',
	'FirstName': 'Colby',
	'MiddleName': 'Michael',
	'LastName': 'King',
})
```
If an identity column is defined on the table you're inserting into, that identity will be returned from the add method:

```python
tbl_address = client.get_table('Address', schema='Person')
new_addr = tbl_person.add({
	'AddressLine1': '123 Easy Street',
	'State': 'GA',
	'Zip': '19334-4422',
	'Apartment Number': 'Colby',
	'IDPerson': new_person 
})
```


### Updating Data 

Records can be updated by the primary key. Pass a dictionary to the update method and make sure the PK is included in the keys. If it is a composite key, you need to specify each field in the PK 

```python
tbl_address = client.get_table('Address', schema='Person')
tbl_address.add({
	'AddressID': new_addr,
	'AddressLine1': '124 Easy Street'
})
```




## Support 

