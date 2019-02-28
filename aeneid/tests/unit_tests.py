import sys
from aeneid.dbservices.RDBDataTable import RDBDataTable

sys.stdout = open('aeneid/testoutput/log.txt', 'w+')

import json
print('test')

def test_create():

    tbl = RDBDataTable("people")
    print("test_create:tbl =",tbl)
    print("HELLO")
test_create()




