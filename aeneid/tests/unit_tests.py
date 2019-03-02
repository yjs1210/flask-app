import sys
from aeneid.dbservices.RDBDataTable import RDBDataTable

sys.stdout = open('aeneid/testoutput/log.txt', 'w+')

import json
print('test')

def test_create():

    tbl = RDBDataTable("people")
    print("test_create:tbl =",tbl)
    print("HELLO")

def test_mapping():
    tbl = RDBDataTable("lahman2017.people",['playerID'])
    print("test_create:tbl =",tbl)
    out = tbl.get_join_column_mapping('lahman2017','appearances')
    print(out)
    print("HELLO")

#test_create()
test_mapping()



