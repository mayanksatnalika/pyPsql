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
        """
        Basic functionality: Set col-name, type and primary key. 
        Table schema a dictionary of format: 
        {"col-name" : "type" ( generic python type for using default values else use postgresql values )} 
        A key with primary key with value same as one of the column names will be set as primary key. 
        """

        python_list_mapping = {
            "int": "integer",
            "str": "text",
            "basestring": "text",
            "unicode": "text",
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

        query = """CREATE TABLE {table_name} ( \n""".format(table_name=table_name) + \
            ", \n".join(["""{col_name}  {col_type}  {other_rules} """.format(col_name=col_name, col_type=sql_converted_schema[col_name], other_rules=additional_rules[col_name]) for col_name in col_names]
                        ) + ")"
        # print query
        try:
            self.conn.cursor().execute(query)
            print "table {table_name} created successfullly.".format(table_name=table_name)
        except Exception as e:
            print "Error in making {err}".format(err=e)

    def table_exists(self, table_name):
        cur = self.conn.cursor()
        cur.execute("select exists(select relname from pg_class where relname='{table_name}')".format(table_name=table_name))

        exists = cur.fetchone()[0]
        # print exists
        return exists
        cur.close()

    def initialize_table(self, table_name):

        if self.table_exists(table_name) is False:
            print "Error table does not exist, make by calling: make_table( '{tname}' ) furnction".format(tname=table_name)
            return 0

        table_object = table_functionality(self, table_name)
        return table_object
        """eval_string = "self.table_ref" + " = refrence_variable"
        print eval_string
        exec(eval_string)
        return self.table_ref
        # self.refrence_variable = refrence_variable """
