##James Lee - jl5241
import pymysql.cursors
import json
import aeneid.utils.utils as ut
import aeneid.utils.dffutils as db
import aeneid.dbservices.DataExceptions
from aeneid.dbservices.RDBDataTable import RDBDataTable

db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.

# Is a dictionary of {table_name : [primary_key_field_1, primary_key_field_2, ...]
# Used to convert a list of column values into a template of the form { col: value }
primary_keys = {}

# This dictionary contains columns mappings for nevigating from a source table to a destination table.
# The keys is of the form sourcename_destinationname. The entry is a list of the form
# [[sourcecolumn1, destinationcolumn1], ...
join_columns = {}

# Data structure contains RI constraints. The format is a dictionary with an entry for each schema.
# Within the schema entry, there is a dictionary containing the constraint name, source and target tables
# and key mappings.
ri_constraints = None

data_tables = {}


# TODO This is a bit of a hack and we should clean up.
# We should load information from database or configuration file.
people = RDBDataTable("lahman2017.people", key_columns=['playerID'])
data_tables["lahman2017.people"] = people
batting = RDBDataTable("lahman2017.batting", key_columns=['playerID', 'teamID', 'yearID', 'stint'])
data_tables["lahman2017.batting"] = batting
appearances = RDBDataTable("lahman2017.appearances", key_columns=['playerID', 'teamID', 'yearID'])
data_tables["lahman2017.appearances"] = appearances
offices = RDBDataTable("classiccars.offices", key_columns=['officeCode'])
data_tables["classiccars.offices"] = offices
fantasy_manager = RDBDataTable("classiccars.fantasy_manager", key_columns=['id'])
data_tables["classiccars.fantasy_manager"] = fantasy_manager
fantasy_team = RDBDataTable("classiccars.fantasy_team", key_columns=['team_id'])
data_tables["classiccars.fantasy_team"] = fantasy_team


def get_data_table(table_name):

    result = data_tables.get(table_name, None)
    if result is None:
        result = RDBDataTable(table_name)
        data_tables[table_name] = result

    return result

def get_by_template(table_name, template,field_list=None, limit=None, offset=None, order_by=None, commit=True,children = None ):

    dt = get_data_table(table_name)
    if children:
        result = dict()
        schema1, table1 = dt._get_schema_table(table_name)
        schema_table1 = schema1+"."+table1
        
        template_parent_org, field_list_parent_org = _get_table_conditions(table_name,template,field_list,subresource=None)
        if field_list: 
            for child in children.split(','):
                foreign_key = get_foreign_key(table_name,child)
                for k,v in foreign_key.items():
                    map = v['MAP'][0]
                    check_in_field = map['REFERENCED_COLUMN_NAME']
                    if table_name.split('.')[1]+'.'+check_in_field not in field_list:
                        field_list.append(table1 + '.'+check_in_field)
        
        template_parent, field_list_parent = _get_table_conditions(table_name,template,field_list,subresource=None)
        
        parent_result = dt.find_by_template(template=template_parent, 
                                            field_list=field_list_parent, 
                                            limit=limit, 
                                            offset=offset,
                                            order_by=None, 
                                            commit=commit,
                                            children=None,
                                            helper=True)
        result[table1] = parent_result.get_rows()
        
        child_fields = dict()
        for child in children.split(','):
            template_child, field_list_child = _get_table_conditions(table_name,template,field_list,child)
            child_result = dt.find_by_template(template=template_child, 
                                            field_list=field_list_child, 
                                            limit=limit, 
                                            offset=offset,
                                            order_by=None, 
                                            commit=commit,
                                            children=child,
                                            helper=True)
            children_rows = child_result.get_rows()
            result[child] = children_rows
            fields_child_org = []
            for i in field_list_child:
                field_tmp = i.split('.')
                if field_tmp[0] == child:
                    fields_child_org.append(field_tmp[1])
            child_fields[child] = fields_child_org

        foreign_keys = dict()
        for k,v in result.items():
            schema2, table2 = dt._get_schema_table(k)
            schema_table2 = schema2+"."+table2
            foreign_keys[k] = get_foreign_key(schema_table1,schema_table2)


        result_out =[]
        for row in result[table1]:
            row_out = dict()
            row_out[table1]= row
            for k,v in result.items():
                if k != table1:
                    foreign_key = foreign_keys[k]
                    child_fields_match = child_fields[k]

                    keys = []
                    for k_map,v_map in foreign_key.items():
                        map = v_map['MAP'][0]
                        keys.append((map['COLUMN_NAME'],map['REFERENCED_COLUMN_NAME']))
                    for match_row in v:
                        match = True
                        for (col,ref) in keys:
                            if match_row[col] != row[ref]:
                                match = False
                        if match:
                            match_out =dict()
                            for row_key,row_val in match_row.items():
                                if row_key in child_fields_match:
                                    match_out[row_key]  = row_val
                            if row_out.get(k,None) is None:
                                row_out[k] = [match_out]
                            else: 
                                row_out[k].append(match_out)
            result_out.append(row_out)
        
        result_seriously_out = []
        for i in result_out:
            if len(i)>1:
                result_seriously_out.append(i)
        

        return result_seriously_out

    else:
        result = dt.find_by_template(template, field_list, limit, offset, order_by, commit,children)




    return result.get_rows()

def _get_table_conditions(table_name, template = None, field_list = None,subresource = None):
        field_filtered =None
        template_filtered = None

        if subresource is None:

            tb_name = table_name.split(".")
            if len(tb_name) ==1:
                tb_name = tb_name[0]
            else:
                tb_name = tb_name[1]
            
            if field_list:
                field_filtered = []
                for field in field_list:
                    field_full = field.split('.')
                    if field_full[0] == tb_name:
                        field_filtered.append(field)

            if template:
                template_filtered = dict()
                for k,v in template.items():
                    clause = k.split('.')
                    if clause[0] == tb_name:
                        template_filtered[k] = v
       
        else:
            tb_name = table_name.split(".")
            if len(tb_name) ==1:
                tb_name = tb_name[0]
            else:
                tb_name = tb_name[1]

            sub_name = subresource.split(".")
            if len(sub_name) ==1:
                sub_name = sub_name[0]
            else:
                sub_name = sub_name[1]
            
            if field_list:
                field_filtered = []
                for field in field_list:
                    field_full = field.split('.')
                    if (field_full[0] == tb_name) | (field_full[0]==sub_name):
                        field_filtered.append(field)

            if template:
                template_filtered = dict()
                for k,v in template.items():
                    clause = k.split('.')
                    if (clause[0] == tb_name) |(clause[0] == sub_name) :
                        template_filtered[k] = v
            
        return template_filtered, field_filtered
                

def get_by_primary_key(table_name, key_fields, field_list=None, commit=True):

    dt = get_data_table(table_name)
    result = dt.find_by_primary_key(key_fields, field_list)
    return result.get_rows()

def get_primary_key_columns(table_name):

    dt = get_data_table(table_name)
    result = dt._get_keys()
    return result

def create(table_name, new_value):
    result = None
    try:
        dt = get_data_table(table_name) 
        result = dt.insert(new_value)
    except Exception as e:
        print("Got exception = ", e)
        raise e
        #map_e = DataTableException(None,None,e)
        #raise map_e
    return result

def delete_by_key(table_name, key_cols):
    dt = get_data_table(table_name)
    result = dt.delete_by_key(key_cols)
    return result 

def update_by_key(table_name, key_fields, new_value):
    dt = get_data_table(table_name)
    result = dt.update_by_key(key_fields,new_value)
    return result

def get_key(table_name):
    dt = get_data_table(table_name)
    result = dt._get_keys()
    return result 


def get_foreign_key(table_name,subresource):
    dt = get_data_table(table_name)
    schema1, table1 = dt._get_schema_table(table_name)
    schema2, table2 = dt._get_schema_table(subresource)
    result = dt.get_join_column_mapping(schema1,table1,schema2,table2)
    return result 














