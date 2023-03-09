from faker import Faker
import os
import random
import datetime

from google.cloud import bigquery
from google.cloud import storage

PROJECT = 'bigquerybook2e'
REGION = 'us-central1'
BUCKET_PREFIX = 'fit-device-events-'
    
bq_client = bigquery.Client(project=PROJECT, location=REGION)

def generate_device_events():
    
    gcs_client = storage.Client()
    
    for bucket in gcs_client.list_buckets(prefix=BUCKET_PREFIX):
        
        bucket_name = bucket.name.replace(BUCKET_PREFIX, '')
        print('bucket_name:', bucket_name)
        
        country_code = bucket_name.split('-')[0] # extract country code from bucket name
        #print('country_code:', country_code)
        
        for i in range(0, random.randint(5, 10)): # number of files
            
            event_date = datetime.date.today()
            event_date_str = event_date.strftime('%Y-%m-%d')
            file_name = event_date_str + '.csv'
            f = open(file_name, 'w')
            f.write('event_time,event_type,device_id,userid,user_email\n')
            generate_user_events(f, event_date_str)
            f.close()
            
            blob_name = 'device_events_' + country_code + '/' + 'dt=' + event_date_str + '/val=' + str(i) + '/' + file_name 
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_name)
            #print('uploaded ' + bucket.name + '/' + blob_name)
            
            os.remove(file_name)


def generate_user_events(f, event_date):
        
    fake = Faker()
    
    event_types = ['firmare_uploaded', 'firmware_update_completed', 'warranty_replaced', 'device_unpaired']
    
    for i in range(0, random.randint(5, 25)): # number of users
    
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
                
                time = fake.time()
                event_time = event_date + 'T' + time + '.000Z'
                
                #print(event_time + ',' + event_type + ',' + device_id + ',' + userid + ',' + user_email)
                f.write(event_time + ',' + event_type + ',' + device_id + ',' + userid + ',' + user_email + '\n')


def append_device_activity_apac():
    
    insert_stmt = ('insert into device.device_activity_daily_apac(event_date, device_id, userid, country_code) ' +
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "au" as country_code '
                	'from device_events_apac.device_events_au where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                	'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "hk" as country_code '
                	'from device_events_apac.device_events_hk where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "in" as country_code '
                	'from device_events_apac.device_events_in where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "jp" as country_code '
                	'from device_events_apac.device_events_jp where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "nz" as country_code '
                	'from device_events_apac.device_events_nz where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "pk" as country_code '
                	'from device_events_apac.device_events_pk where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "sg" as country_code '
                	'from device_events_apac.device_events_sg where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "tw" as country_code '
                	'from device_events_apac.device_events_tw where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date')
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()
    

def append_device_activity_emea():
    
    insert_stmt = ('insert into device.device_activity_daily_emea(event_date, device_id, userid, country_code) ' +
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "dk" as country_code '
                	'from device_events_emea.device_events_dk where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                	'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "fr" as country_code '
                	'from device_events_emea.device_events_fr where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "gh" as country_code '
                	'from device_events_emea.device_events_gh where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "il" as country_code '
                	'from device_events_emea.device_events_il where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "it" as country_code '
                	'from device_events_emea.device_events_it where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ke" as country_code '
                	'from device_events_emea.device_events_ke where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "lu" as country_code '
                	'from device_events_emea.device_events_lu where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "nl" as country_code '
                	'from device_events_emea.device_events_nl where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "no" as country_code '
                	'from device_events_emea.device_events_no where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "nl" as country_code '
                	'from device_events_emea.device_events_nl where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "pl" as country_code '
                	'from device_events_emea.device_events_pl where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ro" as country_code '
                	'from device_events_emea.device_events_ro where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date ' 
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "se" as country_code '
                	'from device_events_emea.device_events_se where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ua" as country_code '
                	'from device_events_emea.device_events_ua where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "uk" as country_code '
                	'from device_events_emea.device_events_uk where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "za" as country_code '
                	'from device_events_emea.device_events_za where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date')
    
    print(insert_stmt)  
    bq_client.query(insert_stmt).result()


def append_device_activity_latam():
    
    insert_stmt = ('insert into device.device_activity_daily_latam(event_date, device_id, userid, country_code) ' +
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ar" as country_code '
                	'from device_events_latam.device_events_ar where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                	'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "br" as country_code '
                	'from device_events_latam.device_events_br where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "co" as country_code '
                	'from device_events_latam.device_events_co where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "do" as country_code '
                	'from device_events_latam.device_events_do where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ec" as country_code '
                	'from device_events_latam.device_events_ec where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "mx" as country_code '
                	'from device_events_latam.device_events_mx where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date '
                    'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "pe" as country_code '
                	'from device_events_latam.device_events_pe where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date')
                    
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()


def append_device_activity_namer():
    
    insert_stmt = ('insert into device.device_activity_daily_namer(event_date, device_id, userid, country_code) ' +
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "ca" as country_code '
                	'from device_events_namer.device_events_ca where dt is not null '
                    'and cast(cast(event_time as timestamp) as date) = current_date '
                	'union all '
                	'select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, "us" as country_code '
                	'from device_events_namer.device_events_us where dt is not null '
                	'and cast(cast(event_time as timestamp) as date) = current_date')

    print(insert_stmt) 
    bq_client.query(insert_stmt).result()
    
   
def append_device_activity_global():

    insert_stmt = ('insert into device.device_activity_daily_global(event_date, device_id, userid, country_code) ' + 
    	           'select * from device.device_activity_daily_apac where event_date = current_date ' +
    	           'union all ' +
    	           'select * from device.device_activity_daily_emea where event_date = current_date ' +
    	           'union all ' +
    	           'select * from device.device_activity_daily_latam where event_date = current_date ' +
    	           'union all ' +
    	           'select * from device.device_activity_daily_namer where event_date = current_date')
    
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()


def append_device_activity_daily_summarized():
    
    insert_stmt = ('insert into device.device_activity_daily_summarized(event_date, country_code, device_cnt, user_cnt) ' +
    	           'select event_date, country_code, count(device_id), count(userid) ' + 
    	           'from device.device_activity_daily_global ' +
                   'where event_date = current_date '
    	           'group by event_date, country_code')
                   
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()

     
def append_device_activity_daily_gender_summarized():
    
    insert_stmt = ('insert into device.device_activity_daily_gender_summarized(event_date, gender, gender_cnt) ' +
               	    'select d.event_date, u.gender, count(*) ' + 
               	    'from device.device_activity_daily_global d join fitdw.User u on d.userid = u.userid ' +
                    'where d.event_date = current_date ' 
               	    'group by d.event_date, u.gender')
                   
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()


def append_device_activity_daily_age_group_summarized():
    
    insert_stmt = ('insert into device.device_activity_daily_age_group_summarized(event_date, age_group, age_group_cnt) ' +
               	    'select d.event_date, u.age_group, count(*) ' + 
               	    'from device.device_activity_daily_global d join fitdw.User u on d.userid = u.userid ' +
                    'where d.event_date = current_date ' 
               	    'group by d.event_date, u.age_group')
                   
    print(insert_stmt) 
    bq_client.query(insert_stmt).result()


if __name__ == '__main__':
    
    generate_device_events()
    append_device_activity_apac()
    append_device_activity_emea()
    append_device_activity_latam()
    append_device_activity_namer()
    append_device_activity_global()
    append_device_activity_daily_summarized()
    append_device_activity_daily_gender_summarized()
    append_device_activity_daily_age_group_summarized()