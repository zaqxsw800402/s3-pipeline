import logging
import pandas as pd
from io import BytesIO, StringIO

import pytest
from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongFormatException

logger = logging.getLogger(__name__)

s3_access_key = 'AWS_ACCESS_KEY_ID'
s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
s3_bucket_name = 'test-bucket'


@pytest.fixture
def s3_bucket_conn():
    yield S3BucketConnector(s3_access_key,
                            s3_secret_key,
                            s3_endpoint_url,
                            s3_bucket_name)


@pytest.fixture(scope='module')
def s3_create_bucket(s3_mock_resource):
    s3_mock_resource.create_bucket(Bucket=s3_bucket_name,
                                   CreateBucketConfiguration={
                                       'LocationConstraint': 'eu-central-1'})


@pytest.fixture(scope='module')
def s3_bucket(s3_mock_resource, s3_create_bucket):
    yield s3_mock_resource.Bucket(s3_bucket_name)


class TestListFilesInPrefix:

    def test_list_files_in_prefix_ok(self, s3_bucket_conn, s3_bucket):
        # s3_bucket = s3_mock_resource.Bucket(s3_bucket_name)

        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """"
        col1,col2
        valA,valB"""
        s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Test after method execution
        assert len(list_result) == 2
        assert key1_exp in list_result
        assert key2_exp in list_result
        # Cleanup after test
        s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    },
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self, s3_bucket_conn, s3_bucket):
        """
        Tests the list_files_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Test init
        prefix = 'no-prefix/'
        # Method execution
        list_result = s3_bucket_conn.list_files_in_prefix(prefix)
        # Test after method execution
        assert not list_result


class TestReadCsvToDf:
    def test_read_csv_to_df_ok(self, s3_bucket, s3_bucket_conn, caplog):
        """
        Tests the read_csv_to_df method for
        reading 1 .csv file from the mocked s3 bucket
        """

        caplog.set_level(logging.INFO)

        # Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {s3_endpoint_url}/{s3_bucket_name}/{key_exp}'
        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        s3_bucket.put_object(Body=csv_content, Key=key_exp)
        # Method execution
        df_result = s3_bucket_conn.read_csv_to_df(key_exp)
        # Log test after method execution
        assert log_exp in caplog.text
        # Test after method execution
        assert df_result.shape[0] == 1
        assert df_result.shape[1] == 2
        assert val1_exp == df_result[col1_exp][0]
        assert val2_exp == df_result[col2_exp][0]
        # Cleanup after test
        s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_read_csv_to_df_bad(self, s3_bucket_conn):
        """There is no csv file in bucket s3"""

        key_exp = 'test.csv'
        with pytest.raises(s3_bucket_conn.session.client('s3').exceptions.NoSuchKey):
            s3_bucket_conn.read_csv_to_df(key_exp)


class TestWriteDfToS3:
    def test_write_df_to_s3_csv(self, s3_bucket_conn, s3_bucket, caplog):
        """
        Tests the write_df_to_s3 method
        if writing csv is successful
        """
        caplog.set_level(logging.INFO)
        # Expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file to {s3_endpoint_url}/{s3_bucket_name}/{key_exp}'
        # Test init
        file_format = 'csv'
        # Method execution
        result = s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
        # Log test after method execution
        assert log_exp in caplog.text
        # Test after method execution
        data = s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        assert return_exp == result
        assert df_exp.equals(df_result)
        # Cleanup after test
        s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_parquet(self, s3_bucket, s3_bucket_conn, caplog):
        """
        Tests the write_df_to_s3 method
        if writing parquet is successful
        """
        caplog.set_level(logging.INFO)
        # Expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file to {s3_endpoint_url}/{s3_bucket_name}/{key_exp}'
        # Test init
        file_format = 'parquet'
        # Method execution
        result = s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
        # Log test after method execution
        assert log_exp in caplog.text
        # Test after method execution
        data = s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        assert return_exp == result
        assert df_exp.equals(df_result)
        # Cleanup after test
        s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_wrong_format(self, s3_bucket_conn, caplog):
        """
        Tests the write_df_to_s3 method
        if a not supported format is given as argument
        """
        caplog.set_level(logging.INFO)
        # Expected results
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        format_exp = 'wrong_format'
        log_exp = f'The file format {format_exp} is not supported to be written to s3!'
        exception_exp = WrongFormatException
        # Method execution
        with pytest.raises(exception_exp):
            s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format_exp)
            # Log test after method execution
            assert log_exp in caplog.text
