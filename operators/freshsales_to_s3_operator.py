from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin
from airflow.hooks.http_hook import HttpHook
from tempfile import NamedTemporaryFile
from dateutil.parser import parse
import json
import logging

class EndpointNotSupported(Exception):

    def __init__(self) -> None:
        super().__init__("Specified endpoint not currently supported.")


class FreshsalesToS3Operator(BaseOperator, SkipMixin):
    """
    Trello to S3 Operator
    :param freshsales_conn_id:      The Airflow id used to store the Freshsales
                                    credentials.
    :type freshsales_conn_id:       string
    :param freshsales_endpoint:     The endpoint to retrive data from.
                                    Implemented for: 
                                        - leads
                                        - contacts
                                        - sales_accounts 
                                        - deals
                                        - tasks
    :type freshsales_endpoint:      string
    :param s3_conn_id:              The Airflow connection id used to store
                                    the S3 credentials.
    :type s3_conn_id:               string
    :param s3_bucket:               The S3 bucket to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    :param s3_key:                  The S3 key to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    :param updated_at:              replication key
    :type:                          datetime
    """

    def __init__(self,
                 freshsales_conn_id,
                 freshsales_endpoint,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 updated_at=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.freshsales_conn_id = freshsales_conn_id
        self.freshsales_endpoint = freshsales_endpoint

        self.updated_at = updated_at

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def filter_result(self, result):
        if (not self.updated_at) or ('updated_at' not in result):
            return True
        return self.updated_at < parse(result['updated_at'])

    def get_views_ids(self, hook, endpoint):
        """
        Get views ids
        """
        results = hook.run(
            "{}/filters".format(endpoint)).json()['filters']

        return [result['id'] for result in results if self.filter_result(result)]

    def get_by_filters(self, hook, endpoint):
        """
        List all objects from the endpoint
        """
        ids = self.get_views_ids(hook, endpoint)
        results = []

        for view_id in ids:
            response = hook.run(
                '{}/view/{}'.format(endpoint, view_id)).json()

            results.extend(response[endpoint])

        return results

    def execute(self, context):
        hook = HttpHook(method='GET', http_conn_id=self.freshsales_conn_id)

        if self.freshsales_endpoint == 'tasks':
            task_filters = ['open', 'due%20today',
                            'due%20tomorrow', 'overdue', 'completed']
            results = []

            for task_filter in task_filters:
                response = hook.run(
                    'tasks?filter={}'.format(task_filter)).json()
                results.extend(response['tasks'])
        else:
            results = self.get_by_filters(hook, self.freshsales_endpoint)

        results = filter(lambda x: self.filter_result(x), results)

        if len(results) == 0 or results is None:
            logging.info("No records pulled from Trello.")
            downstream_tasks = context['task'].get_flat_relatives(
                upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)
            return True

        else:
            # Write the results to a temporary file and save that file to s3.
            with NamedTemporaryFile("w") as tmp:
                for result in results:
                    filtered_result = self.filter_fields(result)
                    tmp.write(json.dumps(filtered_result) + '\n')

                tmp.flush()

                dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )
                dest_s3.connection.close()
                tmp.close()

        
