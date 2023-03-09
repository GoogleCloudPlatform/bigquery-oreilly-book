create schema bigquerybook2e.device
   options (location = 'us-central1');

-- apac tables
create table device.device_activity_daily_apac(event_date date, device_id int, userid string, country_code string) as 
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'au' as country_code
	from device_events_apac.device_events_au where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'hk' as country_code
	from device_events_apac.device_events_hk where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'in' as country_code
	from device_events_apac.device_events_in where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'jp' as country_code
	from device_events_apac.device_events_jp where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'nz' as country_code
	from device_events_apac.device_events_nz where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'pk' as country_code
	from device_events_apac.device_events_pk where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'sg' as country_code
	from device_events_apac.device_events_sg where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'tw' as country_code
	from device_events_apac.device_events_tw where dt is not null;

-- emea tables
create table device.device_activity_daily_emea(event_date date, device_id int, userid string, country_code string) as 
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'dk' as country_code
	from device_events_emea.device_events_dk where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'fr' as country_code
	from device_events_emea.device_events_fr where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'gh' as country_code
	from device_events_emea.device_events_gh where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'il' as country_code
	from device_events_emea.device_events_il where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'it' as country_code
	from device_events_emea.device_events_it where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ke' as country_code
	from device_events_emea.device_events_ke where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'lu' as country_code
	from device_events_emea.device_events_lu where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'nl' as country_code
	from device_events_emea.device_events_nl where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'no' as country_code
	from device_events_emea.device_events_no where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'pl' as country_code
	from device_events_emea.device_events_pl where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ro' as country_code
	from device_events_emea.device_events_ro where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'se' as country_code
	from device_events_emea.device_events_se where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ua' as country_code
	from device_events_emea.device_events_ua where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'uk' as country_code
	from device_events_emea.device_events_uk where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'za' as country_code
	from device_events_emea.device_events_za where dt is not null;

-- latam tables
create table device.device_activity_daily_latam(event_date date, device_id int, userid string, country_code string) as 
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ar' as country_code
	from device_events_latam.device_events_ar where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'br' as country_code
	from device_events_latam.device_events_ar where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'co' as country_code
	from device_events_latam.device_events_co where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'do' as country_code
	from device_events_latam.device_events_do where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ec' as country_code
	from device_events_latam.device_events_ec where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'mx' as country_code
	from device_events_latam.device_events_mx where dt is not null
	union all	
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'pe' as country_code
	from device_events_latam.device_events_pe where dt is not null;
	
-- namer tables
create table device.device_activity_daily_namer(event_date date, device_id int, userid string, country_code string) as 
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'ca' as country_code
	from device_events_namer.device_events_ca where dt is not null
	union all
	select distinct cast(cast(event_time as timestamp) as date) as event_date, device_id, userid, 'us' as country_code
	from device_events_namer.device_events_us where dt is not null;	

-- global table
create table device.device_activity_daily_global as
	select * from device.device_activity_daily_apac
	union all
	select * from device.device_activity_daily_emea
	union all
	select * from device.device_activity_daily_latam
	union all
	select * from device.device_activity_daily_namer;

-- summary tables
create table device.device_activity_daily_summarized as
	select event_date, country_code, count(device_id) as device_cnt, count(userid) as user_cnt
	from device.device_activity_daily_global
	group by event_date, country_code;
	
create table device.device_activity_daily_gender_summarized as
	select d.event_date, u.gender, count(*) as gender_cnt
	from device.device_activity_daily_global d join fitdw.User u on d.userid = u.userid
	group by d.event_date, u.gender;

create table device.device_activity_daily_age_group_summarized as
	select d.event_date, u.age_group, count(*) as age_group_cnt
	from device.device_activity_daily_global d join fitdw.User u on d.userid = u.userid
	group by d.event_date, u.age_group;

-- views 
create view device.device_activity_daily_global_30_days as 
	select * from device.device_activity_daily_global
	where event_date <= date_sub(current_date, INTERVAL 30 DAY);
 
create materialized view device.device_activity_daily_global_90_days as 
	select * from device.device_activity_daily_global
	where event_date <= date_sub('2023-02-16', INTERVAL 90 DAY);
 