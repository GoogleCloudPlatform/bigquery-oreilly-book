INSERT INTO ch02.uk_holiday(holiday_name, holiday_type, holiday_date, weekday) 
	VALUES('Spring Bank Holiday', 'Bank Holiday', '2023-05-29', 'Monday');
	
INSERT INTO ch02.uk_holiday(holiday_name, holiday_type, holiday_date, weekday) 
	VALUES('Spring Bank Holiday', 'Bank Holiday', '2023-05-29', 'Monday'), 
	      ('Battle of the Boyne', 'Bank Holiday', '2023-07-12', 'Wednesday'),
	      ('Summer Bank Holiday', 'Bank Holiday', '2023-08-07', 'Monday');

UPDATE ch02.uk_holiday 
SET holiday_type = 'Local Bank Holiday' 
WHERE holiday_name = 'Battle of the Boyne';  

UPDATE ch02.uk_holiday 
SET weekday = 'Monday' 
WHERE 1 = 1; 
			
DELETE FROM ch02.uk_holiday 
WHERE holiday_name = 'Spring Bank Holiday' 
AND holiday_date = '2023-05-29';

DELETE FROM ch02.uk_holiday
WHERE 1 = 1;

MERGE ch02.uk_holiday MT
USING
 (SELECT *
  FROM ch02.uk_holiday_new) ST
ON MT.holiday_date = ST.holiday_date
AND MT.holiday_type = ST.holiday_type
WHEN MATCHED THEN
  UPDATE
  SET MT.holiday_name = ST.holiday_name, MT.weekday = ST.weekday
WHEN NOT MATCHED BY TARGET THEN
  INSERT(holiday_date, holiday_type, holiday_name, weekday)
  VALUES(ST.holiday_date, ST.holiday_type, ST.holiday_name, ST.weekday);
