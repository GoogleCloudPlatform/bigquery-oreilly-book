#!/usr/bin/env python3
from google.cloud import bigquery

def print_query_results(client,
                        station_name, 
                        min_duration=0, 
                        max_duration=84000):
    print("{} between {} and {}".format(
        station_name, min_duration, max_duration))
    query = """
        SELECT 
          start_station_name
          , AVG(duration) as avg_duration
        FROM 
          `bigquery-public-data`.london_bicycles.cycle_hire
        WHERE 
          start_station_name LIKE CONCAT('%', @STATION, '%')
          AND duration BETWEEN @MIN_DURATION AND @MAX_DURATION
        GROUP BY start_station_name
    """
    query_params = [
        bigquery.ScalarQueryParameter(
            "STATION", "STRING", station_name),
        bigquery.ScalarQueryParameter(
            "MIN_DURATION", "FLOAT64", min_duration),
        bigquery.ScalarQueryParameter(
            "MAX_DURATION", "FLOAT64", max_duration),
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
            row.start_station_name, row.avg_duration))
    print("______________________")



client = bigquery.Client()
print_query_results(client, 'Kennington', 300)
print_query_results(client, 'Hyde Park', 600, 6000)