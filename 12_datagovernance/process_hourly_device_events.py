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

import datetime, os
import psycopg2
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.datacatalog import lineage

PG_INST = 'pgfit'
HOST = "10.42.160.3"
PASS = ";=4'/nRn{ZN*=B_z"
URL = "dbname='postgres' user='postgres' host={} port=5432 password={}".format(HOST, PASS)

PROJECT = "bigquerybook2e"
REGION = 'us-central1'
COUNTRY_MASTER_TABLE = 'fitdw.Country'
BUCKET_PREFIX = 'fit-device-events'

bq_client = bigquery.Client(project=PROJECT, location=REGION)
dl_client = lineage.LineageClient()

def run_controller():
    
    init_bq_tables()
    
    previous_hour = get_previous_hour()
    start_time = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
    
    select_stmt = ("select country_code, region from " + COUNTRY_MASTER_TABLE + " order by region")
    rows = bq_client.query(select_stmt).result()
    
    updates_exist = False
    
    for row in rows:
        country_code = row['country_code']
        region = row['region']
        
        print('Processing', country_code, 'in region', region)
        
        event_tuples = get_device_events_previous_hour(country_code)
        
        if len(event_tuples) == 0:
            print('Country', country_code, 'has no events in previous hour. Please run produce_hourly_device_events.py')
            continue
        
        updates_exist = True
        max_val = derive_partition_layout(region, country_code)
        create_csv_file(country_code, max_val, event_tuples)
        end_time = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
        
        # record lineage
        source_name = 'postgres:{}.device_events.{}'.format(PG_INST, country_code)
        target_name = 'gs://{}-{}/device_events_{}/*'.format(BUCKET_PREFIX, country_code, country_code)
        record_lineage(source_name, target_name, start_time, end_time)
        
        # update summary table (region-level)
        append_device_activity_hourly_region(region, country_code, previous_hour)
    
    # update summary tables (global-level)
    if updates_exist == True:
        append_device_activity_hourly_global(previous_hour)
        append_device_activity_hourly_summarized(previous_hour)
        append_device_activity_hourly_gender_summarized(previous_hour)
        append_device_activity_hourly_age_group_summarized(previous_hour)

        
def init_bq_tables():
    
    hourly_tables_current = []
    
    select_stmt = ("select table_name from device.INFORMATION_SCHEMA.TABLES " +
                   "where table_schema = 'device' and table_name like 'device_activity_hourly_%';")
    rows = bq_client.query(select_stmt).result()
    
    for row in rows:
        hourly_tables_current.append(row['table_name'])
    
    ddl_stmts = []
    
    if 'device_activity_hourly_apac' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_apac(event_hour timestamp, " + 
               "device_id int, userid string, country_code string)")
        ddl_tup = ('device_activity_hourly_apac', ddl)
        ddl_stmts.append(ddl_tup)
    
    if 'device_activity_hourly_emea' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_emea(event_hour timestamp, " + 
                "device_id int, userid string, country_code string)")
        ddl_tup = ('device_activity_hourly_emea', ddl)
        ddl_stmts.append(ddl_tup)
    
    if 'device_activity_hourly_latam' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_latam(event_hour timestamp, " + 
               "device_id int, userid string, country_code string)")
        ddl_tup = ('device_activity_hourly_latam', ddl)
        ddl_stmts.append(ddl_tup)
    
    if 'device_activity_hourly_namer' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_namer(event_hour timestamp, " + 
                         "device_id int, userid string, country_code string)")
        ddl_tup = ('device_activity_hourly_namer', ddl)
        ddl_stmts.append(ddl_tup)

    if 'device_activity_hourly_global' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_global(event_hour timestamp, " + 
               "device_id int, userid string, country_code string)")   
        ddl_tup = ('device_activity_hourly_global', ddl)
        ddl_stmts.append(ddl_tup)

    if 'device_activity_hourly_summarized' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_summarized(event_hour timestamp, " + 
               "country_code string, device_cnt int, user_cnt int)")  
        ddl_tup = ('device_activity_hourly_summarized', ddl)
        ddl_stmts.append(ddl_tup)

    if 'device_activity_hourly_gender_summarized' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_gender_summarized(event_hour timestamp, " + 
               "gender string, gender_cnt int)")    
        ddl_tup = ('device_activity_hourly_gender_summarized', ddl)
        ddl_stmts.append(ddl_tup)

    if 'device_activity_hourly_age_group_summarized' not in hourly_tables_current:
        ddl = ("create table device.device_activity_hourly_age_group_summarized" + 
              "(event_hour timestamp, age_group string, age_group_cnt int)") 
        ddl_tup = ('device_activity_hourly_age_group_summarized', ddl)
        ddl_stmts.append(ddl_tup)
        
    for table_name, ddl_stmt in ddl_stmts:
        result = bq_client.query(ddl_stmt).result()
        print('created table', table_name)
        
    
def get_previous_hour():
    
    sql = ("select extract(hour from (current_time - interval '1 hour')) as previous_hour")

    try:
        conn = psycopg2.connect(URL)

        with conn.cursor() as cur:
            cur.execute(sql)
            previous_hour = cur.fetchone()
            print('previous_hour:', previous_hour[0])
            cur.close()

    except psycopg2.ProgrammingError as e:
        print("Failed to query table {}".format(e))

    return previous_hour[0]    

            
def get_device_events_previous_hour(country_code):
    
    event_tuples = []
    
    sql = ("select to_char(event_time, 'YYYY-MM-DD HH24:MI:SS.00Z'), event_type, device_id, userid, user_email " +
           "from device_events." + country_code + " where event_time::date = current_date and " +
            "extract(hour from event_time) = extract(hour from (current_time - interval '1 hour'))")
    
    #print(sql)
    
    try:
        conn = psycopg2.connect(URL)
        #print(conn)
        
        with conn.cursor() as cur:
            cur.execute(sql)
            event_tuples = cur.fetchall()
            #print('event_tuples:', event_tuples)
            cur.close()

    except psycopg2.ProgrammingError as e:
        print("Failed to query table {}".format(e))

    return event_tuples


def derive_partition_layout(region, country_code):

    select_stmt = ("select max(val) from device_events_" + region + ".device_events_" + country_code +
                   " where dt = cast(current_date as string)")
    
    #print(select_stmt)
    
    rows = bq_client.query(select_stmt).result()
    
    for row in rows:
        max_val = row[0]

    if max_val != None:
        max_val = int(max_val) + 1
    else:
        max_val = 0
    
    return max_val


def create_csv_file(country_code, max_val, event_tuples):
    
    gcs_client = storage.Client()
    
    bucket_prefix = BUCKET_PREFIX + '-' + country_code
    #print('bucket_prefix:', bucket_prefix)
    
    bucket_list = list(gcs_client.list_buckets(prefix=bucket_prefix))
    
    if len(bucket_list) == 0:
        print('Bucket starting with', bucket_prefix, 'not found')
        return -1
    
    bucket = bucket_list[0]
    bucket_name = bucket.name
    print('bucket_name:', bucket_name)
        
    event_date = datetime.date.today()
    event_date_str = event_date.strftime('%Y-%m-%d')
    file_name = event_date_str + '.csv'
    
    f = open(file_name, 'w')
    f.write('event_time,event_type,device_id,userid,user_email\n')

    for event in event_tuples:
        event_time = event[0]
        event_type = event[1]
        device_id = event[2]
        userid = event[3]
        user_email = event[4]
        
        f.write(event_time + "," + event_type + "," + str(device_id) + "," + userid + "," + user_email + "\n")
    
    f.close()
            
    blob_name = 'device_events_' + country_code + '/' + 'dt=' + event_date_str + '/val=' + str(max_val) + '/' + file_name 
    print(blob_name)
        
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_name)
    print('uploaded ' + bucket.name + '/' + blob_name)
            
    os.remove(file_name)

    
def record_lineage(source_name, target_name, start_time, end_time):

    print('source_name:', source_name)
    print('target_name:', target_name)
    print('start_time:', start_time)
    print('end_time:', end_time)
    
    parent = 'projects/{}/locations/{}'.format(PROJECT, REGION)
    print('parent:', parent)
        
    source = lineage.EntityReference()
    source.fully_qualified_name = source_name
    
    target = lineage.EntityReference()
    target.fully_qualified_name = target_name

    search_req = lineage.SearchLinksRequest(
        source=source,
        target=target,
        parent=parent,
    )

    links = list(dl_client.search_links(request=search_req))

    if len(links) > 0:
        link_id = links[0].name
        process = retrieve_process(parent, link_id)
    else:
        process = create_process(parent)
        print('process:', process)
        
    run = create_run(process, start_time, end_time)
    print('run:', run)
        
    create_links(run, source, target, start_time, end_time)
    
    
def retrieve_process(parent, link):
    
    print('retrieve_process')
    print('parent:', parent)
    print('link:', link)
          
    bslpr_req = lineage.BatchSearchLinkProcessesRequest(
        parent = parent,
        links = [link]
    )
    
    bslp_resp = dl_client.batch_search_link_processes(request=bslpr_req)
    #print('bslp_resp:', bslp_resp)
    
    process = None
    
    for process_link in bslp_resp:
        process = process_link.process
    
    return process
    
    
def create_process(parent):
    
    print('create_process')
    print('parent:', parent)
    
    source_type = lineage.Origin.SourceType.CUSTOM
    origin = lineage.Origin(source_type = source_type, name = 'process_hourly_device_events.py')
    process = lineage.Process(display_name = 'Hourly Device Events', origin = origin)
    
    process_req = lineage.CreateProcessRequest(
        parent = parent,
        process = process
    )

    process_resp = dl_client.create_process(request=process_req)
    print(process_resp)
    
    return process_resp.name


def create_run(process, start_time, end_time):
    
    print('create_run')
    
    run = lineage.Run()
    run.state = 'COMPLETED'
    run.display_name = 'process_hourly_device_events.py'
    run.start_time = start_time
    run.end_time = end_time

    run_req = lineage.CreateRunRequest(
        parent=process,
        run=run
    )

    run_resp = dl_client.create_run(request=run_req)
    print(run_resp)
    
    return run_resp.name
 

def create_links(run, source, target, start_time, end_time):
    
    print('create_links')
    
    event_link = lineage.EventLink(source = source, target = target)
    lineage_event = lineage.LineageEvent(links = [event_link], start_time = start_time, end_time = end_time)
    
    links_req = lineage.CreateLineageEventRequest(
        parent = run,
        lineage_event = lineage_event
    )

    links_resp = dl_client.create_lineage_event(request=links_req)
    print('links_resp:', links_resp)

    
def append_device_activity_hourly_region(region, country_code, previous_hour):

    insert = ("insert into device.device_activity_hourly_{}(event_hour, device_id, userid, country_code) " +
              "select timestamp_trunc(cast(event_time as timestamp), hour) as event_hour, device_id, userid, " + 
              "'{}' as country_code " +
              "from device_events_{}.device_events_{} where dt is not null " +
              "and cast(cast(event_time as timestamp) as date) = current_date() " + 
              "and timestamp_trunc(cast(event_time as timestamp), hour) = timestamp(concat(current_date(), " +
              "' {}:00:00 UTC'))").format(region, country_code, region, country_code, previous_hour)
    
    print(insert)
    
    bq_client.query(insert).result()

def append_device_activity_hourly_global(previous_hour):

    insert_stmt = ('insert into device.device_activity_hourly_global(event_hour, device_id, userid, country_code) ' + 
                   'select * from device.device_activity_hourly_apac where ' +
                   'timestamp_trunc(cast(event_hour as timestamp), hour) = ' +
                   'timestamp(concat(current_date(), " {}:00:00 UTC")) ' +
                   'union all ' +
                   'select * from device.device_activity_hourly_emea where ' +
                   'timestamp_trunc(cast(event_hour as timestamp), hour) = ' +
                   'timestamp(concat(current_date(), " {}:00:00 UTC")) ' +
                   'union all ' +
                   'select * from device.device_activity_hourly_latam where ' +
                   'timestamp_trunc(cast(event_hour as timestamp), hour) = ' +
                   'timestamp(concat(current_date(), " {}:00:00 UTC")) ' +
                   'union all ' +
                   'select * from device.device_activity_hourly_namer where ' +
                   'timestamp_trunc(cast(event_hour as timestamp), hour) = ' +
                   'timestamp(concat(current_date(), " {}:00:00 UTC")) ').format(previous_hour, previous_hour,
                                                                                 previous_hour, previous_hour)
                   
    
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()

    
def append_device_activity_hourly_summarized(previous_hour):
    
    insert = ('insert into device.device_activity_hourly_summarized(event_hour, country_code, device_cnt, ' +
              'user_cnt) ' +
              'select event_hour, country_code, count(device_id), count(userid) ' + 
              'from device.device_activity_hourly_global ' +
              'where event_hour =  '
              'timestamp(concat(current_date(), " {}:00:00 UTC")) ' +
              'group by event_hour, country_code').format(previous_hour)
                   
    print(insert) 
    bq_client.query(insert).result()

     
def append_device_activity_hourly_gender_summarized(previous_hour):
    
    insert = ('insert into device.device_activity_hourly_gender_summarized(event_hour, gender, gender_cnt) ' +
              'select h.event_hour, u.gender, count(*) ' + 
              'from device.device_activity_hourly_global h join fitdw.User u on h.userid = u.userid ' +
              'where h.event_hour = timestamp(concat(current_date(), " {}:00:00 UTC")) ' 
              'group by h.event_hour, u.gender').format(previous_hour)
                   
    print(insert) 
    bq_client.query(insert).result()


def append_device_activity_hourly_age_group_summarized(previous_hour):
    
    insert = ('insert into device.device_activity_hourly_age_group_summarized(event_hour, age_group, ' + 
              'age_group_cnt) ' +
              'select h.event_hour, u.age_group, count(*) ' + 
              'from device.device_activity_hourly_global h join fitdw.User u on h.userid = u.userid ' +
              'where h.event_hour = timestamp(concat(current_date(), " {}:00:00 UTC")) ' 
              'group by h.event_hour, u.age_group').format(previous_hour)
                   
    print(insert) 
    bq_client.query(insert).result()

    
if __name__ == '__main__':
    #run_controller()
    source_name = 'postgres:pgfit.device_events.ar'
    target_name = 'gs://fit-device-events-ar/device_events_ar/*'
    record_lineage(source_name, target_name, None, None)
