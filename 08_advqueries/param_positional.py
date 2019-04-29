#!/usr/bin/env python3
from google.cloud import bigquery

def print_query_results(client, params):
    query = """
        SELECT 
          start_station_name
          , AVG(duration) as avg_duration
        FROM 
          `bigquery-public-data`.london_bicycles.cycle_hire
        WHERE 
          start_station_name LIKE CONCAT('%', ?, '%')
          AND duration BETWEEN ? AND ?
        GROUP BY start_station_name
    """
    query_params = [
        bigquery.ScalarQueryParameter(
            None, "STRING", params[0]),
        bigquery.ScalarQueryParameter(
            None, "FLOAT64", params[1]),
        bigquery.ScalarQueryParameter(
            None, "FLOAT64", params[2]),
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
print_query_results(client, ['Kennington', 300, 84000])
print_query_results(client, ['Hyde Park', 600, 6000])