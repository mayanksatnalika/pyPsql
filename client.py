# coding: utf-8
import psycopg2
from table_functionality import table_functionality


class db_connection():
    def __init__(self, user, password, host, port, database_name):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name
        self.table_ref_variable = None
        try:
            self.conn = psycopg2.connect(user=self.user,  password=self.password, host=self.host,  port=self.port, dbname=self.database_name)
            self.conn.autocommit = True
        except Exception as e:
            print ("Error in making connection, error: {err}".format(err=e))

    def make_table(self, table_name, table_schema, primary_key=[None], unique=[None], not_null=[None]):
        """Basic functionality: makes a new table if it does not exist.

        Arguments:
            table_name {string} -- Name of new table
            table_schema {dict} -- dictionary denoting col-names and col-data type.
            Ex: {"name":"string" }  ( generic python type for using default values else use postgresql values )

        Keyword Arguments:
            primary_key {list of strings } -- List of col-names to be set as primay key in new table (default: {[None]})
            unique {list of strings } -- List of col-names to be set as unique in new table (default: {[None]})
            not_null {list of strings} -- List of col-names to be set as not_null in new table (default: {[None]})

        """
        python_list_mapping = {
            "int": "integer",
            "str": "text",
            "float": "NUMERIC(5)",
            "bool": "boolean",
            "date": "date"

        }

        col_names = table_schema.keys()
        sql_converted_schema = {}
        additional_rules = {}

        for col_name in col_names:
            additional_rules[col_name] = ""
            if table_schema[col_name] in python_list_mapping:
                sql_converted_schema[col_name] = python_list_mapping[table_schema[col_name]]
            else:
                sql_converted_schema[col_name] = table_schema[col_name]
            if col_name == primary_key:
                additional_rules[col_name] = additional_rules[col_name] + " PRIMARY KEY"
            if col_name in unique:
                additional_rules[col_name] = additional_rules[col_name] + " UNIQUE"
            if col_name in not_null:
                additional_rules[col_name] = additional_rules[col_name] + " NOT NULL"

        query = """CREATE TABLE {table_name} ( \n""".format(table_name=table_name) + ", \n".join(["""{col_name}  {col_type}  {other_rules} """.format(
            col_name=col_name, col_type=sql_converted_schema[col_name], other_rules=additional_rules[col_name]) for col_name in col_names]) + ")"
        print query
        try:
            self.conn.cursor().execute(query)
        except Exception as e:
            print "Error in making {err}".format(err=e)

    def auto_table(self, table_name, data, primary_key=[None], unique=[None], not_null=[None]):
        """
        Create a new table compatiable with the data.

        Arguments:
            table_name {string} -- The new table name
            data {dict} -- A payload which is to be inserted in the new table.

        Keyword Arguments:
            primary_key {list of strings } -- List of keys from data() to be set as primay key in new table (default: {[None]})
            unique {list of strings } -- List of keys from data() to be set as unique in new table (default: {[None]})
            not_null {list of strings} -- List of keys from data() to be set as not_null in new table (default: {[None]})
        """
        # Todo.
        pass

    def table_exists(self, table_name):
        """Checks if table existsa and returns boolean true/ false

        Arguments:
            table_name {string} -- name of table to check

        """

        cur = self.conn.cursor()
        cur.execute("select exists(select relname from pg_class where relname='{table_name}')".format(table_name=table_name))

        exists = cur.fetchone()[0]
        print exists
        return exists
        cur.close()

    def initialize_table(self, table_name):

        if self.table_exists(table_name) is False:
            err = "Error table does not exist, make by calling: make_table( '{tname}' ) function".format(tname=table_name)
            return err

        table_object = table_functionality(self, table_name)
        return table_object


mycon = db_connection(user="admin",  password="admin", host="127.0.0.1",  port="5432",    database_name="mydb")
tableclient = mycon.initialize_table("mobile")
print tableclient.get_table_description()
