import urllib2
import json 
import time 
from glob import glob 
import sys 
import sqlite3
conn = sqlite3.connect('requests.sqlite')
cursor = conn.cursor()

# First, let's define some functions to query the history sources

def parse_record(path):
    with open(path,'r') as f:
        payload = json.load(f)['phedex']
    records = []
    for request in payload['request']:
        datasets = [(x['name'], x['bytes']) for x in request['data']['dbs']['dataset']]
        blocks = [(x['name'], x['bytes']) for x in request['data']['dbs']['block']]
        objs = datasets + blocks 
        user = request['requested_by']['name']
            
        for node in request['destinations']['node']:
            decided_by = node['decided_by']
            if not (decided_by['decision'] == 'y'):
                continue 
            site = node['name']
            timestamp = decided_by['time_decided']
            
            for obj in objs:
                if not obj[1]:
                    continue
                records.append((obj[0], int(obj[1]/1e6), site, user, int(timestamp)))
        
        return records
    
def create_table():
    cursor.execute('DROP TABLE IF EXISTS requests')
    cursor.execute('CREATE TABLE requests (object TEXT, size INTEGER, site TEXT, user TEXT, timestamp INTEGER)')
    
def insert_record(path):
    records = parse_record(path)
    cursor.executemany('INSERT INTO requests VALUES (?,?,?,?,?)', records)
    conn.commit()

# aggregate the data into a database
create_table()
paths = glob('data/*')
for i, path in enumerate(paths):
    if i%100 == 0:
        print '%i/%i'%(i, len(paths))
        
    insert_record(path)
conn.commit()
conn.close()
