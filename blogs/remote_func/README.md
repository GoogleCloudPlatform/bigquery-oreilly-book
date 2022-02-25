
## Instructions

* Visit https://console.developers.google.com/apis/api/bigqueryconnection.googleapis.com/
* Enable the BigQuery Connection API

* In CloudShell
  * gcloud components update
  * bq show blogs || bq mk -d blogs
  * bq mk --connection --display_name='add_fake_user' --connection_type=CLOUD_RESOURCE --project_id=$(gcloud config get-value project) --location=US  gcf-conn
  * bq show --location=US --connection gcf-conn
  * Make sure to note the service account id

* Visit https://console.cloud.google.com/functions

* Create a Cloud Function
  * Call the function add_fake_user
  * Accept all other defaults
  * Change code to Python 3.x
  * Change entry-point to add_fake_user
  * copy-paste code the following code into the editor:
    ```
    import json

    def add_fake_user(request):
      request_json = request.get_json(silent=True)
      replies = []
      calls = request_json['calls']
      for call in calls:
        userno = call[0]
        corp = call[1]
        replies.append({
          'username': f'user_{userno}',
          'email': f'user_{userno}@{corp}.com'
        })
      return json.dumps({
        # each reply is a STRING (JSON not currently supported)
        'replies': [json.dumps(reply) for reply in replies]
      })
    ```
  * Deploy

* Try out the Cloud Function
  * Switch to the Testing Tab of the cloud function
  * copy-paste the following into the editor tab:
      ```
      {
        "calls": [
            [4557, "acme"],
            [8756, "hilltop"]
        ]
      }
      ``` 
  * Click on the test button
  * You should get a JSON whose main element is "replies"

* Allow BigQuery to call the Cloud Function
  * Switch to the Permissions tab
  * Add the serviceAccountID as a Cloud Function Invoker

* Now go to BigQuery console https://console.cloud.google.com/bigquery

* Create a SQL function that in turn will invoke the Cloud Function (note the two places to change)
    ```
    CREATE OR REPLACE FUNCTION blogs.add_fake_user(user_id int64, corp_id STRING) RETURNS STRING
    REMOTE WITH CONNECTION `vivid-tuner-338922.us.gcf-conn` -- change this to reflect your PROJECT ID
    OPTIONS (
        -- change this to reflect the Trigger URL of your cloud function (look for the TRIGGER tab)
        endpoint = 'https://us-central1-vivid-tuner-338922.cloudfunctions.net/add_fake_user'
    )
    ```
* Now, you can use it from any SQL function
    ```
    SELECT
      start_station_id, end_station_id, 
      blogs.add_fake_user(trip_id, bikeid)
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
    LIMIT 10
    ```
* You can parse the string using JSON_QUERY:
    ```
    WITH data AS (
          SELECT
            trip_id, bikeid, 
            start_station_id, end_station_id, 
            blogs.add_fake_user(trip_id, bikeid) AS reply
          FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
          LIMIT 10
    )
    
    SELECT 
      * EXCEPT(reply),
      JSON_QUERY(reply, '$.email') AS user_id
    FROM data
    ```

* Or you can use the new JSON built-in type:
   ```
    WITH data AS (
          SELECT
            trip_id, bikeid, 
            start_station_id, end_station_id, 
            TO_JSON(blogs.add_fake_user(trip_id, bikeid)) AS reply
          FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
          LIMIT 10
    )
    
    SELECT 
      * EXCEPT(reply),
      reply.email
    FROM data
    ```   



