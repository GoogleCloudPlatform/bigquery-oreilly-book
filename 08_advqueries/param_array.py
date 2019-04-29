#!/usr/bin/env python3
from google.cloud import bigquery

def print_query_results(client, ids, min_duration=0, max_duration=84000):
    query = """
        SELECT 
          start_station_id
          , COUNT(*) as num_trips
        FROM 
          `bigquery-public-data`.london_bicycles.cycle_hire
        WHERE 
          start_station_id IN UNNEST(@STATIONS)
          AND duration BETWEEN @MIN_DURATION AND @MAX_DURATION
        GROUP BY start_station_id
    """
    query_params = [
        bigquery.ArrayQueryParameter(
            'STATIONS', "INT64", ids),
        bigquery.ScalarQueryParameter(
            'MIN_DURATION', "FLOAT64", min_duration),
        bigquery.ScalarQueryParameter(
            'MAX_DURATION', "FLOAT64", max_duration),
    ]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(
        query,
        location="EU",
        job_config=job_config,
    )
    for row in query_job:
        print("{}: \t{}".format(
            row.start_station_id, row.num_trips))
    print("______________________")



client = bigquery.Client()
print_query_results(client, [270, 235, 62, 149], 300, 600)
