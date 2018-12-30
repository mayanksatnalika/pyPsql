# pyPsql
### A wrapper around psycopg2 to get a PyMongo(esqe) interface for interacting  postgresql databases. 

##### What it is? 
A wrapper around psycopg2 package for Postgresql databases which provides basic functionalities in a style similar to pyMongo. This provides the basic functionality of creating tables, read and insert. 
##### Why I wrote this?
I started development using noSQL databases and shifting from using mongodb (with pyMongo) to PostgreSQL (using psycopg2) in a few personal projects was a huge pain.
##### Dependencies:
[`psycopg2 2.7.6.1`](https://pypi.org/project/psycopg2/) and above, should probably work with older versions as well, this was the latest version at the time I made this.
##### Usage:

0. Import: 
	`from pypsql import client
`

1. Open a new connection:
 
	```
    mycon  = client.db_connection(user = "user",  password = "pwd", host = "127.0.0.1",  port = "5432",    database_name  = 	"mydb")
    ``` 
    This is same as using:
    ```
    client = MongoClient('localhost', 27017)`
	db_client = client.test_db
    ```
    in PyMongo.
2. Create a new table:
	```
	mycon.make_table(table_name = "my_new_table", table_schema ={"id":"int","name":"str"})
    ```
    The table schema is a dictionary with keys as coumn names, and values as type of that column name. The type
    is of string type and can either be generic python types or a complex postgresql type.
3. Create a object to link to table:
	```
    client  = mycon.initialize_table("my_new_table")
    ```
    
    This is same as using `collection = db_client.newcoll`  in PyMongo. 
    
4. Inserting: 

	```
    client.insert({"id" : 1,"model" : "abcd"})
	```
5. Querying:
	```
    values = client.query(field_names= ["id"]) # GET 'id' FIELD.
    values = client.query( )  					# GET ALL FIELD.
    ```
