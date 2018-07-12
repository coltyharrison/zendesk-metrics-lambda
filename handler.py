import base64
import boto3
import json
import os

from botocore.vendored import requests

s3 = boto3.resource('s3')


class ZendeskConnector(object):

    def __init__(self):
        self.headers = self._get_headers()
        self.metrics_url = self._get_metrics_url()
        self.rows = []
        self.organizations = {}
        self.users = {}
        self.fields = [
            'Date Created',
            'Ticket ID',
            'Assignee Name',
            'Ticket Subject',
            'Organization',
            'Type',
            'Trello Card',
            'Reason Code',
            'Product Area'
        ]

    def _get_headers(self):
        return {
            'Authorization': 'Basic {}'.format(base64.b64encode(
                bytes(os.environ['ZENDESK_TOKEN'], 'utf-8')
            ).decode('utf-8'))
        }

    def _get_metrics_url(self):
        return '{}/api/v2/views/{}/execute.json'.format(
            os.environ['ZENDESK_URL'], os.environ['ZENDESK_VIEW'])

    def _get_request(self, url):
        r = requests.get(url, headers=self.headers)
        return r.json()

    def _update_metrics_data(self, response):
        self.rows.extend(response['rows'])
        self.organizations.update(
            {org['id']: org['name'] for org in response['organizations']})
        self.users.update(
            {user['id']: user['name'] for user in response['users']})

    def _create_data_table(self):
        data_table = []
        for row in self.rows:
            data_table.append([
                row['created'].split('T')[0],  # date created

                row['ticket']['id'],  # ticket id

                self.users[row['assignee_id']],  # assignee name

                row['ticket']['subject'],  # ticket subject

                self.organizations[row['organization_id']]\
                if row['organization_id'] else '',  # organization

                row['custom_fields'][0]['name']\
                if row['custom_fields'][0] else '',  # type

                row['custom_fields'][1]['name']\
                if row['custom_fields'][1] else '',  # trello card

                row['custom_fields'][2]['value']\
                if row['custom_fields'][2] else '',  # reason code

                row['custom_fields'][3]['name']\
                if row['custom_fields'][3] else '',  # product area
            ])
        return {
            "data": data_table,
            "fields": self.fields
        }

    def _prep_raw_data(self):
        return {'rows': self.rows,
                'organizations': self.organizations,
                'users': self.users}

    def _write_to_s3(self, **kwargs):
        for k, v in kwargs.items():
            s3.Bucket(os.environ['S3_BUCKET_NAME']).put_object(
                Key='data/zendesk_{}.json'.format(k),
                Body=json.dumps(v)
            )

    def get_and_store_metrics_in_s3(self):
        response = self._get_request(self.metrics_url)
        self._update_metrics_data(response)
        while response['next_page']:
            response = self._get_request(response['next_page'])
            self._update_metrics_data(response)
        self._write_to_s3(
            raw_data=self._prep_raw_data(),
            data_tables=self._create_data_table()
        )


def lambda_handler(event, context):
    zendesk = ZendeskConnector()
    zendesk.get_and_store_metrics_in_s3()
    return 'Hello from Lambda'
