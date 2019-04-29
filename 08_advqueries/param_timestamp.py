#!/usr/bin/env python3
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
import pytz

def print_query_results(client, mid_time):
    start_time = mid_time - timedelta(minutes=30)
    end_time = mid_time + timedelta(minutes=30)
    print('Between {} and {}'.format(start_time, end_time))
    query = """
        SELECT 
          AVG(duration) as avg_duration
        FROM 
          `bigquery-public-data`.london_bicycles.cycle_hire
        WHERE 
          start_date BETWEEN @START_TIME AND @END_TIME
    """
    query_params = [
        bigquery.ScalarQueryParameter(
            "START_TIME", "TIMESTAMP", start_time),
        bigquery.ScalarQueryParameter(
            "END_TIME", "TIMESTAMP", end_time),
    ]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(
        query,
        location="EU",
        job_config=job_config,
    )
    for row in query_job:
        print(row.avg_duration)
    print("______________________")



client = bigquery.Client()
print_query_results(client, datetime(2015, 12, 25, 15, 0, tzinfo=pytz.UTC))
print_query_results(client, datetime(2016, 12, 25, 15, 0, tzinfo=pytz.UTC))