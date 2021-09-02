import boto3
import pytest
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector

s3_access_key = 'AWS_ACCESS_KEY_ID'
s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
s3_bucket_name = 'test-bucket'


@pytest.fixture(scope='module')
def s3_mock_resource():
    with mock_s3():
        mock_resource = boto3.resource(service_name='s3', endpoint_url=s3_endpoint_url)
        yield mock_resource


@pytest.fixture
def s3_bucket_conn():
    yield S3BucketConnector(s3_access_key,
                            s3_secret_key,
                            s3_endpoint_url,
                            s3_bucket_name)

# @pytest.fixture
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#
#
# @pytest.fixture(scope='session')
# def s3_client(aws_credentials):
#     with mock_s3():
#         conn = boto3.client("s3", region_name="us-east-1")
#         yield conn
