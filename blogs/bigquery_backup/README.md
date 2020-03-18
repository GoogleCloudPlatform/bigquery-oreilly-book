## Backup (and restore) a BigQuery table or dataset

Accompanies the blog post:
https://medium.com/google-cloud/how-to-backup-a-bigquery-table-or-dataset-to-google-cloud-storage-and-restore-from-it-6ef7eb322c6d

### Example usage

1. Backup a table to GCS
```
./bq_backup.py --input dataset.tablename --output gs://BUCKET/backup
```
This saves a schema.json, a tabledef.json, and extracted data in AVRO format to GCS.


2. You can also backup all the tables in a data set:
```
./bq_backup.py --input dataset --output gs://BUCKET/backup
```

3. Restore tables one-by-one by specifying a destination data set
```
./bq_restore.py --input gs://BUCKET/backup/fromdataset/fromtable --output destdataset
```

For views, the backup stores the view definition and the restore creates a view.
The data behind the view is not backed up.


### Dependencies
These require gcloud and bq command-line utilities to be installed.