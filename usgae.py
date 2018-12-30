from pypsql import client
mycon = client.db_connection(user="admin",  password="admin", host="127.0.0.1",  port="5432",    database_name="mydb")
mycon.make_table(table_name="my_new_table", table_schema={"id": "int", "name": "str"})
client = mycon.initialize_table("my_new_table")
client.insert({"id": 1, "name": "abcd"})
values = client.query()  					# GET ALL FIELD.
print values
