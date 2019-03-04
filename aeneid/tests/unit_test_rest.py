##James Lee - jl5241
import requests
import sys
sys.stdout = open('aeneid/test_results/rest_log.txt', 'w+')
import json
print("##James Lee - jl5241\n")
print("************************************************************\n")
print("************PLEASE NOTE THAT I HAD TO DROP SOME ROWS TO GET FOREIGN KEY WORKING**************\n")
print("************************************************************\n")

def display_response(rsp):
    try:
        print("URL GIVEN =" + rsp.url)
        print("Printing a response.")
        print("HTTP status code: ", rsp.status_code)
        h = dict(rsp.headers)
        print("Response headers: \n", json.dumps(h, indent=2, default=str))

        try:
            body = rsp.json()
            print("JSON body: \n", json.dumps(body, indent=2, default=str))
        except Exception as e:
            body = rsp.text
            print("Text body: \n", body)

    except Exception as e:
        print("display_response got exception e = ", e)

def test_api_1():
    print("*************TEST ONE API CONNECTION TEST**************")
    r=requests.get("http://127.0.0.1:5000")
    result = r.text
    display_response(r)

def test_json():
    print("*************TEST TWO GET BY RESOURCE/TEMPLATE TEST**************")
    params = { "nameLast": "Williams", "fields": "playerID, nameLast, nameFirst"}
    url = 'http://127.0.0.1:5000/api/lahman2017/people'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers, params = params)
    print("Select playerID, nameLast, nameFirst from   lahman2017.players \
    where nameLast = Williams ")
    display_response(r)

def test_order_by():
    print("*************TEST THREE ORDER BY**************")
    url = 'http://127.0.0.1:5000/api/lahman2017/batting?teamID=BOS&limit=10&offset=10&fields=playerID,yearID,H,AB,RBI&order_by=yearID'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers)
    display_response(r)


def test_get_by_key():

    print("*************TEST FOUR GET BY PRIMARY KEY TEST SAME AS HW DOCUMENTATION **************")
    url = 'http://127.0.0.1:5000/api/lahman2017/appearances/willite01_BOS_1960?fields=G_all,GS'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers)
    print("GET G_all and GS BY COMPOUND KEY willite01_BOS_1960")
    display_response(r)

def test_complex_query1():

    print("*************TEST FIVE COMPELX QUERY TEST SAME RESULT AS PIAZZA **************")
    url = 'http://127.0.0.1:5000/api/lahman2017/people?children=appearances,batting&fields=people.playerID,people.nameLast,people.nameFirst,people.birthCity,batting.teamID,batting.yearID,batting.H,batting.HR,batting.RBI,appearances.G_all,appearances.teamID,appearances.yearID&people.nameLast=Williams&people.birthCity=San Diego'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers)
    print("Result = ")
    display_response(r)

def test_complex_query2():


    print("*************TEST SIX COMPELX QUERY TEST 2 SAME RESULT AS PIAZZA **************")
    url = 'http://127.0.0.1:5000/api/lahman2017/people?children=appearances,batting&fields=people.playerID,people.nameLast,people.nameFirst,people.birthCity,batting.teamID,batting.yearID,batting.H,batting.HR,batting.RBI,appearances.G_all,appearances.teamID,appearances.yearID&people.nameLast=Williams&batting.yearID=1960&appearances.yearID=1960'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers)
    display_response(r)


def test_query_by_key_and_subresource():

    print("*************TEST SEVEN QUERY BY KEY AND SUBRESOURCE SAME AS HW DOCUMENTATION**************")
    url = 'http://127.0.0.1:5000/api/lahman2017/people/willite01/batting?fields=ab,h&yearid=1960'
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    r = requests.get(url, headers=headers)
    display_response(r)

def test_create():
    print("*************TEST EIGHT CREATE SAME AS PIAZZA**************")
    
    body = {
            "id": "ok1",
            "last_name": "Obiwan",
            "first_name": "Kenobi",
            "email": "ow@jedi.org"
        }
    print("\ntest_create_manager: test 1, manager = \,", json.dumps(body, indent=2, default=str))
    url = "http://127.0.0.1:5000/api/classiccars/fantasy_manager"
    headers = {"content-type": "application/json"}
    result = requests.post(url, headers=headers, json=body)
    display_response(result)

    print("\ntest_create_manager: test 2 retrieving created manager.")
    link = result.headers.get('Location', None)
    if link is None:
        print("No link header returned.")
    else:
        url = link
        headers = None
        result = requests.get(url)
        display_response(result)


    print("\ntest_create_manager: test 1, creating duplicate = \,", json.dumps(body, indent=2, default=str))
    url = "http://127.0.0.1:5000/api/classiccars/fantasy_manager"
    headers = {"content-type": "application/json"}
    result = requests.post(url, headers=headers, json=body)
    display_response(result)


def test_update_manager():

    print("*************TEST NINE SIMPLE UPDATE TEST SAME AS PIAZZA **************")
    try:

        print("\ntest_update_manager: test 1, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/classiccars/fantasy_manager/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

        body = {
            "last_name": "Darth",
            "first_name": "Vader",
            "email": "dv@deathstar.navy.mil"
        }

        print("\ntest_update_manager: test 2, updating data with new value = ")
        print(json.dumps(body))
        headers = {"Content-Type": "application/json"}
        result = requests.put(url, headers=headers, json=body)
        print("\ntest_update_manager: test 2, updating response =  ")
        display_response(result)

        print("\ntest_update_manager: test 3, get manager with id = ls1")
        url = "http://127.0.0.1:5000/api/classiccars/fantasy_manager/ls1"
        result = requests.get(url)
        print("test_update_manager: get result = ")
        display_response(result)

    except Exception as e:
        print("PUT got exception = ", e)

def test_get_by_path():

    
    
    print("*************TEST TEN GET BY PATH SAME AS PIAZZA **************")
   
    try:

        team_id=27
        sub_resource = "fantasy_manager"

        print("\ntest_get_by_path: test 1")
        print("team_id = ", team_id)
        print("sub_resource = ", sub_resource)

        path_url = "http://127.0.0.1:5000/api/classiccars/fantasy_team/" + str(team_id) + "/" + sub_resource
        print("Path = ", path_url)
        result = requests.get(path_url)
        print("test_get_by_path: path_url = ")
        display_response(result)


    except Exception as e:
        print("PUT got exception = ", e)


def test_create_related():
    print("*************TEST ELEVEN CREATE RELATED SAME AS PIAZZA **************")

    try:

        playerid = 'ls1'
        sub_resource = "fantasy_team"

        print("\ntest_create_related: test 1")
        path_url = "http://127.0.0.1:5000/api/classiccars/fantasy_manager/ls1/fantasy_team"
        print("Path = ", path_url)
        body = {"team_name": "Braves", "team_id":11}
        print("Body = \n", json.dumps(body, indent=2))
        result = requests.post(path_url, json=body, headers={"Content-Type" : "application/json"})
        print("test_create_related response = ")
        display_response(result)

        l = result.headers.get("Location", None)
        if l is not None:
            print("Got location = ", l)
            print("test_create_related, getting new resource")
            result = requests.get(l)
            display_response(result)
        else:
            print("No location?")


    except Exception as e:
        print("POST got exception = ", e)


test_api_1()
test_json()
test_order_by()
test_get_by_key()
test_complex_query1()
test_complex_query2()
test_query_by_key_and_subresource()
test_create()
test_update_manager()
test_get_by_path()
test_create_related()