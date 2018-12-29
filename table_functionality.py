# coding: utf-8


class table_functionality():
    def __init__(self, dbconn, table_name):
        self.dbconn = dbconn
        self.table_name = table_name

    def get_table_description(self):

        query = "select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name='{table_name}'".format(table_name=self.table_name)
        cur = self.dbconn.conn.cursor()
        cur.execute(query)
        col_details = cur.fetchall()
        return_description = {}
        for det_ in col_details:
            name_, type_, max_len_ = det_
            return_description[str(name_)] = {"data-type": type_, "max_len": max_len_}

        return return_description

    def is_valid_data(self, data):

        return True

    def insert(self, data, ignore_extra=True):
        if not is_valid_data(data):
            return """Invalid data/ you may need to update table schema/
                    create new table with this data input type.
                    call auto_table(data, primary_key = [None], unique = [None], not_null = [None] )"""

        query_string = "INSERT INTO {TABLE_NAME} VALUES ( ".format(TABLE_NAME=self.table_name) + ",".join(col_name for col_name in data.keys()
                                                                                                          if col_name in table_schema) + " ) values ( " + ",".join(data[col_name] for col_name in data.keys() if col_name in table_schema)

        print query_string

        cur = dbconn.conn.cursor()
        cur.execute(query_string)

        exists = cur.fetchone()[0]
