import anapioficeandfire
import json
import requests
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import json
from pandas.io.json import json_normalize


"CHARACTERS"

next_url = "https://anapioficeandfire.com/api/characters/"

characters=[]

while next_url:
    resp = requests.get(next_url)
    if resp.ok:
        characters += resp.json()
        # Get the next page link from the Link header. See https://tools.ietf.org/html/rfc5988
        next_url = resp.links.get('next', {}).get('url')
    else:
        print ("Request failed with {}").format(resp.status_code)
        sys.exit(1)



def create_table1():
   conn=psycopg2.connect("dbname='GoT' user='postgres' password='HVpWGgjXi8soQFKAG9zO' host='localhost' port='5432' ") 
   cur=conn.cursor() 
   cur.execute("CREATE TABLE IF NOT EXISTS characters (url TEXT, name TEXT, gender TEXT, born TEXT,died TEXT,titles TEXT,aliases TEXT , father TEXT,mother TEXT,spouse TEXT ,allegiances TEXT,books TEXT, povBooks TEXT, tvSeries TEXT,playedBy TEXT   )")
   conn.commit() 
   conn.close() 

create_table1()

df = json_normalize(characters)

engine = create_engine("postgresql+psycopg2://postgres:HVpWGgjXi8soQFKAG9zO@localhost:5432/GoT")
df.to_sql("characters", engine, index=False, if_exists='replace')
print("Done")



"BOOKS"
next_url = "https://anapioficeandfire.com/api/books"
books=[]
while next_url:
    resp = requests.get(next_url)
    if resp.ok:
        books += resp.json()
        # Get the next page link from the Link header. See https://tools.ietf.org/html/rfc5988
        next_url = resp.links.get('next', {}).get('url')
    else:
        print ("Request failed with {}").format(resp.status_code)
        sys.exit(1)
def create_table3():
   conn=psycopg2.connect("dbname='GoT' user='postgres' password='HVpWGgjXi8soQFKAG9zO' host='localhost' port='5432' ") 
   cur=conn.cursor() 
   cur.execute("CREATE TABLE IF NOT EXISTS books (url TEXT, name TEXT, isbn TEXT, authors TEXT,numberOfPages TEXT,publisher TEXT,country TEXT , mediaType TEXT,released TEXT,characters TEXT ,povCharacters TEXT  )")
   conn.commit() 
   conn.close() 

create_table3()

df = json_normalize(books)



engine = create_engine("postgresql+psycopg2://postgres:HVpWGgjXi8soQFKAG9zO@localhost:5432/GoT")
df.to_sql("books", engine, index=False, if_exists='replace')
print("Done")



"HOUSES"

next_url = "https://anapioficeandfire.com/api/houses"
houses=[]
while next_url:
    resp = requests.get(next_url)
    if resp.ok:
        houses += resp.json()
        # Get the next page link from the Link header. See https://tools.ietf.org/html/rfc5988
        next_url = resp.links.get('next', {}).get('url')
    else:
        print ("Request failed with {}")
        sys.exit(1)
def create_table4():
   conn=psycopg2.connect("dbname='GoT' user='postgres' password='HVpWGgjXi8soQFKAG9zO' host='localhost' port='5432' ") 
   cur=conn.cursor() 
   cur.execute("CREATE TABLE IF NOT EXISTS houses (url TEXT, name TEXT, region TEXT, coatOfArms TEXT,words TEXT,titles TEXT,seats TEXT , currentLord TEXT,heir TEXT,overlord TEXT ,founded TEXT,founder TEXT,diedOut TEXT,ancestralWeapons TEXT , cadetBranches TEXT,swornMembers TEXT )")
   conn.commit() 
   conn.close() 

create_table4()

df = json_normalize(houses)

engine = create_engine("postgresql+psycopg2://postgres:HVpWGgjXi8soQFKAG9zO@localhost:5432/GoT")
df.to_sql("houses", engine, index=False, if_exists='replace')
print("Done")



