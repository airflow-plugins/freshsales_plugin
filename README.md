# Plugin - Freshsales to S3

This plugin moves data from the [Freshsales](https://www.freshsales.io/api) API to S3. Implemented for leads, contacts, sales_accounts, deals and tasks

## Hooks
### HttpHook
[Core Airflow HttpHook](https://github.com/apache/incubator-airflow/blob/master/airflow/hooks/http_hook.py)

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### FreshsalesToS3Operator
This operator composes the logic for this plugin. It fetches a specific endpoint and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

- `freshdesk_conn_id`: The Airflow id used to store the Trello credentials.
- `freshdesk_endpoint`: The endpoint to retrive data from.
- `s3_conn_id`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.  
- `updated_at`: *optional* dattetime string used as replication key
