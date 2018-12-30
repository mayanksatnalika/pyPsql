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

    def query(self,  field_names=[], query=" ", jsonify=False):
        # loads todo:
        # 1.basic: select * from table
        # 2. select a,b from table
        # 3. select a,b from table where a =b
        # 4. select a,b from table where a=b, b > c
        # 5. goes wild. using multiple tables...

        if field_names == []:
            col_names = "*"
        else:
            col_names = ",".join(['"' + fname + '"' for fname in field_names])
        query = "select {col_names} from {table_name} ".format(col_names=col_names, table_name=self.table_name)
        # print query
        cur = self.dbconn.conn.cursor()
        cur.execute(query)
        cur.execute(query)
        vals = cur.fetchall()
        return vals

    def is_valid_data(self, data, ignore_extra=True):
        # To-do maintain a extensive list of possible python to coressponding sql and vice-versa type mapping and use for natching.
        # Using a basic dictionry at the moment.
        table_schema = self.get_table_description()

        python_list_mapping = {
            "int": "integer",
            "str": "text",
            "basestring": "text",
            "unicode": "text",
            "float": "NUMERIC(5)",
            "bool": "boolean",
            "date": "date"

        }

        reverse_python_type_matching = {
            "integer": ["int"],
            "text": ["str", "unicode"],
            "NUMERIC(5)": ["float"],
            "real": ["float", "int"],
            "boolean": ["bool"],
            "date": ["date"]

        }

        for row_ in data.keys():
            """print row_
            print type(data[row_]).__name__
            print reverse_python_type_matching[table_schema[row_]['data-type']]"""

            if row_ in table_schema.keys():
                if type(data[row_]).__name__ not in reverse_python_type_matching[table_schema[row_]['data-type']]:
                    print "type mismatch"
                    return False
                if table_schema[row_]['max_len'] is not None:
                    print "length mismatch"
                    if len(str(data[row_])) > table_schema[row_]['max_len']:
                        return False
            else:
                if ignore_extra is False:
                    print "extra field {row_name } present.".format(row_name=str(row_))
                    return False

        return True

    def remove_extra_fields(self,  data):
        # Todo
        return data

    def insert(self, data, ignore_extra=True):

        if not self.is_valid_data(data, ignore_extra):
            return """Invalid data/ you may need to update table schema/
                    create new table with this data input type.
                    call auto_table(data, primary_key = [None], unique = [None], not_null = [None] )"""

        data = self.remove_extra_fields(data)

        table_schema = self.get_table_description()

        insert_dict = {}
        for col_name in data.keys():
            if table_schema[col_name]['data-type'] in ["text"]:
                insert_dict['"'+col_name+'"'] = "'" + data[col_name] + "'"
            else:
                insert_dict['"'+col_name+'"'] = data[col_name]

        query_string = "INSERT INTO {TABLE_NAME}  ( ".format(TABLE_NAME=self.table_name) + ",".join(col_name for col_name in insert_dict.keys()) + \
            " ) values ( " + ",".join(str(insert_dict[col_name]) for col_name in insert_dict.keys()) + ")"

        # print query_string

        cur = self.dbconn.conn.cursor()
        cur.execute(query_string)
        return True
