""" Connect to s3"""
import os
import logging
import boto3


class S3BucketConnector:
    """connect to s3"""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str):
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource('s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket_name)

    def list_files_in_prefix(self, prefix: str):
        """ list all files with a prefix on the bucket"""
        return [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]

    def read_csv_to_df(self):
        pass

    def write_df_to_s3(self):
        pass
