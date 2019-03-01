from aeneid.dbservices.BaseDataTable import BaseDataTable
from aeneid.dbservices.DerivedDataTable import DerivedDataTable
import pandas as pd

import pymysql


class RDBDataTable(BaseDataTable):
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    #
    # You must replace with your own schema, user id and password.
    #
    _default_connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuser',
        'db': 'lahman2017',
        'port': 3306
    }

    def debug_message(self, *m):
        """
        Prints some debug information if self._debug is True
        :param m: List of things to print.
        :return: None
        """
        if self._debug:
            print(" *** DEBUG:", *m)
    
    def _get_keys(self):
        t_name = self._table_name
        actual_keys = self._run_q("SHOW KEYS FROM " + t_name + " WHERE Key_name = 'PRIMARY'")
        actual_key_columns = [i['Column_name'] for i in actual_keys]
        return actual_key_columns

    def _check_connection(self):
        out = self._run_q("Select 'something'")
        if out != [{'something': 'something'}]:
            raise ValueError('Invalid Connection String')
        
        
    def _check_keys_init(self, keys=None):
        ###check if the keys inputted by user is valid
        if keys is None:
            key_check = self._key_columns
        else:
            key_check = keys
        actual_key_columns = self._get_keys()
        if (actual_key_columns!=key_check):
            raise ValueError("You entered the wrong keys please try "+ ', '.join(str(p) for p in actual_key_columns)) 
            
    def _check_keys_query(self, keys=None):
        ###check if the keys inputted by user is valid
        actual_keys =  self._key_columns
        if keys is None:
            if not actual_keys is None:
                raise ValueError("You entered the wrong keys please try "+ ', '.join(str(p) for p in actual_keys)) 
        else: 
            if actual_keys is None:
                raise ValueError("Key column not initiated, you can't query by the keys")
            else:
                if len(actual_keys) != len(keys):
                    raise ValueError("You entered wrong length keys, please try "+ ', '.join(str(p) for p in actual_keys)) 
    
    def _get_fields(self):
        t_name = self._table_name
        actual = self._run_q("DESCRIBE " + t_name)
        out = [i['Field'] for i in actual]
        return out
    
    def _check_fields(self,fields):
        actual_fields = self._get_fields()
        for i in fields:
            if (not i in actual_fields):
                raise ValueError("Field "+ i+ " does not exist")
        

    def __init__(self, table_name, key_columns, connect_info=None, debug=True):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        # Initialize and store information in the parent class.
        super().__init__(table_name, connect_info, key_columns, debug)
        

        # If there is not explicit connect information, use the defaults.
        if connect_info is None:
            self._connect_info = RDBDataTable._default_connect_info

        # Create a connection to use inside this object. In general, this is not the right approach.
        # There would be a connection pool shared across many classes and applications.
        self._cnx = pymysql.connect(
            host=self._connect_info['host'],
            user=self._connect_info['user'],
            password=self._connect_info['password'],
            db=self._connect_info['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        
        self._check_connection()
        self._check_keys_init(key_columns)
        self.debug = debug
        
    def __str__(self):
        """

                :return: String representation of the data table.
                """
        result = "RDBDataTable: table_name = " + self._table_name
        result += "\nKey fields: " + str(self._key_columns)

        # Find out how many rows are in the table.
        q1 = "SELECT count(*) as count from " + self._table_name
        r1 = self._run_q(q1, fetch=True, commit=True)
        result += "\nNo. of rows = " + str(r1[0]['count'])

        # Get the first five rows and print to show sample data.
        # Query to get data.
        q = "select * from " + self._table_name + " limit 5"

        # Read into a data frame to make pretty print easier.
        df = pd.read_sql(q, self._cnx)
        result += "\nFirst five rows:\n"
        result += df.to_string()

        return result

    def _run_q(self, q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """

        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = self._cnx

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        self.debug_message("Query = ", cursor.mogrify(q, args))
        
        num = cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            r = num

        if commit:                  # Do not worry about this for now.
            cnx.commit()
        
        
        
        return r

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=True):
        """

        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        try:
            q = "insert into " + table_name + " "

            # If the column list is not None, form the (col1, col2, ...) part of the statement.
            if column_list is not None:
                q += "(" + ",".join(column_list) + ") "

            # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
            # each value to insert.
            values = ["%s"] * len(values_list)

            # Form the values(%s, %s, ...) part of the statement.
            values = " ( " + ",".join(values) + ") "
            values = "values" + values

            # Put all together.
            q += values
            
            return self._run_q(q, args=values_list, fields=None, fetch=False, cnx=cnx, commit=commit)

        except Exception as e:
            print("Got exception = ", e)
            raise e

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """
        # Your implementation goes here.
        try:
            self._check_keys_query(key_fields)
            key_columns = self._get_keys()            
            template =  dict(zip(key_columns, key_fields))
            return self.find_by_template(template, field_list=field_list, limit=None, offset=None, order_by=None, commit=True)

        except Exception as e:
            print("Got exception = ", e)
            raise e
               
    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        w_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            w_clause = "WHERE " + " AND ".join(terms)
        else:
            w_clause = ""
            args = None

        return w_clause, args

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = None

        try:
            # Compute the where clause.
            w_clause = self._template_to_where_clause(template)

            if field_list is None:
                # If there is not field list, we are doing 'select *'
                f_select = ['*']
            else:
                f_select = field_list

           # ext = self._get_extra(limit,offset,order_by)
            # Query template.
            # _run_q will use args for %s terms and will format the field selection into {}
            q = "select {} from " + self._table_name + " " + w_clause[0] 
            #" " + ext

            if order_by:
                q += " order by " + str(order_by)
            if limit:
                q += " limit " + str(limit)
            if offset:
                q += " offset " + str(offset)

            result = self._run_q(q, args=w_clause[1], fields=f_select, fetch=True, commit=commit)            

            # SELECT queries always produce tables.
            result = DerivedDataTable("SELECT(" + self._table_name + ")", result)

        except Exception as e:
            print("Exception e = ", e)
            raise e

        return result


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        try:
            c_list = list(new_record.keys())
            v_list = list(new_record.values())
            return self._run_insert(self._table_name, c_list, v_list)
        except Exception as e:
            print("insert: Exception e = ", e)
            raise(e)

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        t_name = self._table_name             
                               
        try:
            
            w_clause, args_ = self._template_to_where_clause(template)
            q = "DELETE FROM " + t_name + " " + w_clause
            nums = self._run_q(q, args=args_, fields=None, fetch=False, cnx=None, commit=True)

        except Exception as e:
            print("Got exception = ", e)
            raise e
        
        
        return nums

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        t_name = self._table_name  
        
                               
        try:
            
            if self._key_columns is None:
                keys = self._run_q("SHOW KEYS FROM " + t_name + " WHERE Key_name = 'PRIMARY'")
                key_columns = [i['Column_name'] for i in keys]     
            else: 
                key_columns = self._key_columns 
            
            if len(key_columns) != len(key_fields):
                raise KeyError('Wrong Length Keys')
            
            # Beginning part of the SQL statement.
            template =  dict(zip(key_columns, key_fields))
            return self.delete_by_template(template)

        except Exception as e:
            print("Got exception = ", e)
            raise e
    
    def _template_to_update_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        u_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            u_clause = " SET " + " , ".join(terms)
        else:
            u_clause = ""
            args = None

        return u_clause, args
    
    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        t_name = self._table_name             
                               
        try:
            
            w_clause, args_w = self._template_to_where_clause(template)
            u_clause, args_u = self._template_to_update_clause(new_values)
            args_ = args_u+args_w
            q = "UPDATE " + t_name + " " + u_clause +" "+ w_clause
            return self._run_q(q, args= args_, fields=None, fetch=False, cnx=None, commit=True)

        except Exception as e:
            print("Got exception = ", e)
            raise e
        

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        t_name = self._table_name   
   
        try:
            
            if self._key_columns is None:
                keys = self._run_q("SHOW KEYS FROM " + t_name + " WHERE Key_name = 'PRIMARY'")
                key_columns = [i['Column_name'] for i in keys]     
            else: 
                key_columns = self._key_columns 
            
            if len(key_columns) != len(key_fields):
                raise KeyError('Wrong Length Keys')

            # Beginning part of the SQL statement.
            template =  dict(zip(key_columns, key_fields))
            self.update_by_template(template, new_values)       

        except Exception as e:
            print("Got exception = ", e)
            raise e

