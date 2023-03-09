from faker import Faker
import datetime
import time
import random
import os
import argparse

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import dataplex

fake = Faker()
gcs_client = storage.Client()
dp_client = dataplex.DataplexServiceClient()
BUCKET_PREFIX = 'fit-device-events-'

def create_buckets(project, location, country_table):
    
    buckets = []
    bq_client = bigquery.Client(project=project, location=location)
    select_stmt = "select country_code, region from " + country_table + ' order by region'
    rows = bq_client.query(select_stmt).result()

    for row in rows:
        name = BUCKET_PREFIX + row['country_code'] + '-' + project
        bucket = gcs_client.bucket(name)

        try:
            bucket.create(location=location)
            buckets.append({'bucket_name': name, 'country_code': row['country_code'], 'region': row['region']})
            print('created bucket', name)
        
        except Exception as e:
            #print(e)
            if 'Your previous request to create the named bucket succeeded and you already own it' in str(e):
                buckets.append({'bucket_name': name, 'country_code': row['country_code'], 'region': row['region']})
    
    return buckets

def retrieve_buckets(project, location, country_table):
    
    country_region_mappings = {}
    buckets = []
    
    bq_client = bigquery.Client(project=project, location=location)
    select_stmt = "select country_code, region from " + country_table + ' order by region'
    rows = bq_client.query(select_stmt).result()

    for row in rows:
        country_region_mappings[row['country_code']] = row['region']
    
    for bucket in gcs_client.list_buckets(prefix=BUCKET_PREFIX):
        
        bucket_name = bucket.name.replace(BUCKET_PREFIX, '')
        country_code = bucket_name.split('-')[0] # extract country code from bucket name
        buckets.append({'bucket_name': bucket.name, 'country_code': country_code, 'region': country_region_mappings[country_code]})
    
    return buckets

def generate_events(bucket_list):
    
    for bucket_entry in bucket_list:
        
        event_date = None
        
        for i in range(0, 10): # number of files
            
            if i == 0:
                event_date = fake.date_between_dates(datetime.date(2018, 1, 1), datetime.date.today()) 
            else:
                event_date = fake.date_between_dates(event_date, datetime.date.today())
            
            event_date_str = event_date.strftime('%Y-%m-%d')
            file_name = event_date_str + '.csv'        
            #print('file_name:', file_name)
            
            f = open(file_name, 'w')
            f.write('event_time,event_type,device_id,userid,user_email\n')
            generate_user_events(f, event_date_str)
            f.close()
        
            bucket = gcs_client.get_bucket(bucket_entry['bucket_name'])
            blob_name = 'device_events_' + bucket_entry['country_code'] + '/' + 'dt=' + event_date_str + '/val=' + str(i) + '/' + file_name 

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_name)
            print('uploaded ' + bucket_entry['bucket_name'] + '/' + blob_name)
            
            os.remove(file_name)

def generate_user_events(f, event_date):
        
    event_types = ['device_updated', 'device_paired', 'device_unpaired', 'validation_completed', 'validation_succeeded', 'validation_failed']
    
    for i in range(0, 10): # number of users
    
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
            
        else:
            userid = fake.first_name_nonbinary().lower()
            user_email = fake.email()
            
        # device id
        device_id = str(random.randint(10000000, 99999999))
        
        for j in range(0, 5): # number of entries per user 
            
            if j == 0:
                event_type = 'device_added'
            elif j == 1:
                event_type = 'validation_started'
            else:
                event_type = random.choice(event_types)
                
            time = fake.time()
            event_time = event_date + 'T' + time + '.000Z'

            f.write(event_time + ',' + event_type + ',' + device_id + ',' + userid + ',' + user_email + '\n')
 
 
def create_lakes(project, region):
    
    parent = 'projects/' + project + '/locations/' + region
    lake_ids = ['device', 'user', 'activities', 'social']

    for lake_id in lake_ids:
        
        request = dataplex.CreateLakeRequest(
            parent=parent,
            lake_id=lake_id,
            )

        try:
            oper = dp_client.create_lake(request=request)
            print('Creating lake ' + lake_id + '. This is a long operation.')
            resp = oper.result()
            print('Lake creation completed with status of', resp.state)
        
        except exceptions.AlreadyExists as ae:
            print('Skipping lake', lake_id, 'as it already exists.')
            
    
def create_zones(project, region):
    
    device_lake = 'projects/' + project + '/locations/' + region + '/lakes/device'
    zone_ids = ['device-events-apac', 'device-events-emea', 'device-events-latam', 'device-events-namer']
    zones = []
    
    for zone_id in zone_ids:

        zone = dataplex.Zone()
        zone.type_ = 'RAW'
        zone.resource_spec.location_type = 'SINGLE_REGION'
        zone.discovery_spec.csv_options.header_rows = 1
        zone.discovery_spec.csv_options.delimiter = ','
        zone.discovery_spec.enabled = True

        request = dataplex.CreateZoneRequest(
            parent=device_lake,
            zone_id=zone_id,
            zone=zone,
        )

        try:
            oper = dp_client.create_zone(request=request)
            resp = oper.result()
            
            print('Zone', zone_id, 'created with status of', resp.state)
            zones.append(resp.name)
        
        except exceptions.InvalidArgument as e:
             
             if 'is already in use in this project' in str(e):
                 #print('Zone', zone_id, 'already exists')
                 zones.append(device_lake + '/zones/' + zone_id)
    
    return zones
 
 
def create_assets(project, region, buckets, zones):
    
    device_lake = 'projects/' + project + '/locations/' + region + '/lakes/device'
    
    for zone in zones:
        
        current_assets = []
        req = dataplex.ListAssetsRequest(
                parent=device_lake + '/zones/' + zone,
            )
        res = dp_client.list_assets(request=req)

        for asset in res:
            current_assets.append(asset.name)

        for bucket in buckets:
        
            if bucket.get('region') not in zone:
                continue
        
            bucket_id = bucket.get('bucket_name')
            asset_id = device_lake + '/zones/' + zone + '/assets/' + bucket_id

            asset_exists = False    
            for ast in current_assets:
                if ast in asset_id:
                    asset_exists = True
                    print('Asset', asset_id, 'already exists')
                    break    
            
            if asset_exists:
                continue
                
            asset = dataplex.Asset()
            asset.resource_spec.type_ = 'STORAGE_BUCKET'
            asset.resource_spec.name = 'projects/{}/buckets/{}'.format(project, bucket_id)

            req = dataplex.CreateAssetRequest(
                parent=device_lake + '/zones/' + zone,
                asset_id=bucket_id,
                asset=asset,
            )

            try:
                oper = dp_client.create_asset(request=req)
                print('Creating asset ' + bucket_id + ' in region ' + bucket.get('region') + '. This is a long operation.')
                resp = oper.result()
                print('Asset', bucket_id, 'was created with status', resp.state)
                #time.sleep(60) # sleep for 60 seconds due to avoid quota issues given that the default write quota is low 
                
            except exceptions.InvalidArgument as ia:
                if 'is already attached' in str(ia):
                    print('Asset ' + bucket_id + ' already exists in region ' + bucket.get('region') + '.')
    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Creates and populates a Dataplex lake.")
    parser.add_argument('mode', help='The mode with which to run this script. Options are: create_buckets, generate_logs, create_lakes, create_zones, create_assets, full_run')
    parser.add_argument('project', help='The Google Cloud project id to use.')
    parser.add_argument('region', help='The Google Cloud region to use.')
    parser.add_argument('country_table', help='The Country table in BigQuery. Should be equal to \'fitdw.Country\' if you ran the create_populate_country.sql')
    args = parser.parse_args()
    
    if args.mode == 'create_buckets':
        buckets = create_buckets(args.project, args.region, args.country_table)
    
    elif args.mode == 'generate_logs':
        buckets = create_buckets(args.project, args.region, args.country_table)
        generate_events(buckets)
    
    elif args.mode == 'create_lakes':
        create_lakes(args.project, args.region)
    
    elif args.mode == 'create_zones':
        create_zones(args.project, args.region)
    
    elif args.mode == 'create_assets':
        buckets = retrieve_buckets(args.project, args.region, args.country_table)
        zones = ['device-events-apac', 'device-events-emea', 'device-events-latam', 'device-events-namer']
        create_assets(args.project, args.region, buckets, zones)
    
    elif args.mode == 'full_run':
        buckets = create_buckets(args.project, args.region, args.country_table)
        generate_events(buckets)
        create_lakes(args.project, args.region)
        zones = create_zones(args.project, args.region)
        create_assets(args.project, buckets, zones)
    
    else:
        print('Invalid mode option. The mode parameter needs to be one of these types: create_buckets, generate_logs, create_lakes, create_zones, create_assets, full_run')