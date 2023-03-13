--
-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

create or replace table fitdw.Country(
	country_code STRING,
	region STRING
);

insert into fitdw.Country(country_code, region) values('us', 'namer');
insert into fitdw.Country(country_code, region) values('ca', 'namer');
insert into fitdw.Country(country_code, region) values('mx', 'namer');
insert into fitdw.Country(country_code, region) values('br', 'latam');
insert into fitdw.Country(country_code, region) values('ar', 'latam');
insert into fitdw.Country(country_code, region) values('co', 'latam');
insert into fitdw.Country(country_code, region) values('pe', 'latam');
insert into fitdw.Country(country_code, region) values('ec', 'latam');
insert into fitdw.Country(country_code, region) values('do', 'latam');
insert into fitdw.Country(country_code, region) values('uk', 'emea');
insert into fitdw.Country(country_code, region) values('fr', 'emea');
insert into fitdw.Country(country_code, region) values('it', 'emea');
insert into fitdw.Country(country_code, region) values('il', 'emea');
insert into fitdw.Country(country_code, region) values('de', 'emea');
insert into fitdw.Country(country_code, region) values('pl', 'emea');
insert into fitdw.Country(country_code, region) values('ua', 'emea');
insert into fitdw.Country(country_code, region) values('ch', 'emea');
insert into fitdw.Country(country_code, region) values('nl', 'emea');
insert into fitdw.Country(country_code, region) values('se', 'emea');
insert into fitdw.Country(country_code, region) values('dk', 'emea');
insert into fitdw.Country(country_code, region) values('no', 'emea');
insert into fitdw.Country(country_code, region) values('be', 'emea');
insert into fitdw.Country(country_code, region) values('lu', 'emea');
insert into fitdw.Country(country_code, region) values('at', 'emea');
insert into fitdw.Country(country_code, region) values('gh', 'emea');
insert into fitdw.Country(country_code, region) values('za', 'emea');
insert into fitdw.Country(country_code, region) values('ke', 'emea');
insert into fitdw.Country(country_code, region) values('bg', 'emea');
insert into fitdw.Country(country_code, region) values('ro', 'emea');
insert into fitdw.Country(country_code, region) values('in', 'apac');
insert into fitdw.Country(country_code, region) values('pk', 'apac');
insert into fitdw.Country(country_code, region) values('jp', 'apac');
insert into fitdw.Country(country_code, region) values('hk', 'apac');
insert into fitdw.Country(country_code, region) values('sg', 'apac');
insert into fitdw.Country(country_code, region) values('tw', 'apac');
insert into fitdw.Country(country_code, region) values('au', 'apac');
insert into fitdw.Country(country_code, region) values('nz', 'apac');
