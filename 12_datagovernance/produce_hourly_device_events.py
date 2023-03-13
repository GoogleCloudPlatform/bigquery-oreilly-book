#!/usr/bin/python
#
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from faker import Faker
import datetime
import random
import psycopg2
import psycopg2.extras

from google.cloud import bigquery
from google.cloud import storage

HOST = "10.42.160.3"
PASS = ";=4'/nRn{ZN*=B_z"
URL = "dbname='postgres' user='postgres' host={} port=5432 password={}".format(HOST, PASS)

PROJECT = "bigquerybook2e"
REGION = 'us-central1'
COUNTRY_MASTER_TABLE = "fitdw.Country"

bq_client = bigquery.Client(project=PROJECT, location=REGION)
conn = psycopg2.connect(URL)


def run_controller():
    
    init_pg_schema()
        
    select_stmt = ("select country_code from " + COUNTRY_MASTER_TABLE)
    
    countries = bq_client.query(select_stmt).result()
    
    for country_code in countries:

        create_pg_table(country_code[0])
        populate_pg_table(country_code[0])
            

def init_pg_schema():

    drop_ddl = ("drop schema if exists device_events cascade")
    create_ddl = ("create schema device_events")
    
    try:
    
        with conn.cursor() as cur:
            cur.execute(drop_ddl)
            cur.execute(create_ddl)
            cur.close()

    except psycopg2.errors.OperationalError as error:
        print("Failed to create table {}".format(error))
        
            
def create_pg_table(country_code):

    ddl = ("create table if not exists device_events." + country_code + 
           " (event_time timestamp, event_type varchar(30), device_id int, " + 
           "userid varchar(100), user_email varchar(100))")
    try:
    
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.close()

    except psycopg2.errors.OperationalError as error:
        print("Failed to create table {}".format(error))
    
    
def populate_pg_table(country_code):
    
    fake = Faker()
    
    insert = ("insert into device_events." + country_code +  "(event_time, event_type, device_id, userid, user_email) " +
            "values(now() - interval '1 hour', %s, %s, %s, %s)")
    
    #print(insert)
    
    event_types = ['firmare_uploaded', 'firmware_update_completed', 'warranty_replaced', 'device_unpaired']
    
    for i in range(0, random.randint(5, 25)): # number of users
    
        insert_tuples = []
        
        # userid and user_email
        gender = random.choice(['M', 'F', 'O'])

        if gender == 'M':
            profile = fake.simple_profile(sex='M')
            userid = profile['username']
            user_email = profile['mail']   
        
        elif gender == 'F':
            profile = fake.simple_profile(sex='F')
            userid = profile['username']
            user_email = profile['mail']   
            
        elif gender == 'O':
            userid = fake.first_name_nonbinary().lower()
            user_email = fake.email()
          
        for j in range(0, random.randint(2, 5)): # number of devices per user
        
            device_id = str(random.randint(10000000, 99999999))
        
            for k in range(0, random.randint(3, 6)): # number of log entries per user 
            
                if k == 0:
                    event_type = 'device_paired'
                elif k == 1:
                    event_type = 'firmware_update_started'
                else:
                    event_type = random.choice(event_types)
                
                insert_tuple = (event_type, device_id, userid, user_email,)
                #print(insert_tuple)
                
                insert_tuples.append(insert_tuple)
           
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert, insert_tuples)
                conn.commit()
                print('inserted', len(insert_tuples), 'into the device_events.' + country_code, 'table') 
                insert_tuples.clear()

        except psycopg2.ProgrammingError as e:
            print("Failed to insert rows {}".format(e))

           
if __name__ == '__main__':
   run_controller()

