"""TestMetaProcessMethods"""
import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import pytest

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.meta_process import MetaProcess

s3_access_key = 'AWS_ACCESS_KEY_ID'
s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
s3_bucket_name = 'test-bucket'


@pytest.fixture(scope='module')
def s3_create_bucket(s3_mock_resource):
    s3_mock_resource.create_bucket(Bucket=s3_bucket_name,
                                   CreateBucketConfiguration={
                                       'LocationConstraint': 'eu-central-1'})


@pytest.fixture(scope='module', autouse=True)
def s3_meta_bucket(s3_mock_resource, s3_create_bucket):
    yield s3_mock_resource.Bucket(s3_bucket_name)


class TestMetaUpdateFile:
    """
    Testing the MetaProcess class.
    """

    def test_update_meta_file_no_meta_file(self, s3_meta_bucket, s3_bucket_conn):
        """
        Tests the update_meta_file method
        when there is no meta file
        """
        # Expected results
        date_list_exp = ['2021-04-16', '2021-04-17']
        proc_date_list_exp = [datetime.today().date()] * 2
        # Test init
        meta_key = 'meta.csv'
        # Method execution
        MetaProcess.update_meta_file(date_list_exp, meta_key, s3_bucket_conn)
        # MetaProcess.update_meta_file(date_list_exp, meta_key, self.s3_bucket_meta)

        # Read meta file
        data = s3_meta_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test after method execution
        assert date_list_exp == date_list_result
        assert proc_date_list_exp == proc_date_list_result
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self, s3_bucket_conn, caplog):
        """
        Tests the update_meta_file method
        when the argument extract_date_list is empty
        """
        caplog.set_level(logging.INFO)
        # Expected results
        return_exp = True
        log_exp = 'The dataframe is empty! No file will be written!'
        # Test init
        date_list = []
        meta_key = 'meta.csv'
        # Method execution
        result = MetaProcess.update_meta_file(date_list, meta_key, s3_bucket_conn)
        # Log test after method execution
        assert log_exp in caplog.text
        # Test after method execution
        assert return_exp == result

    def test_update_meta_file_meta_file_ok(self, s3_bucket_conn, s3_meta_bucket):
        """
        Tests the update_meta_file method
        when there already a meta file
        """
        # Expected results
        date_list_old = ['2021-04-12', '2021-04-13']
        date_list_new = ['2021-04-16', '2021-04-17']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        s3_meta_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        MetaProcess.update_meta_file(date_list_new, meta_key, s3_bucket_conn)
        # Read meta file
        data = s3_meta_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[
                                    MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[
                                                        MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test after method execution
        assert date_list_exp == date_list_result
        assert proc_date_list_exp == proc_date_list_result
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_meta_file_wrong(self, s3_bucket_conn, s3_meta_bucket):
        """
        Tests the update_meta_file method
        when there is a wrong meta file
        """
        # Expected results
        date_list_old = ['2021-04-12', '2021-04-13']
        date_list_new = ['2021-04-16', '2021-04-17']
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        s3_meta_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        with pytest.raises(WrongMetaFileException):
            MetaProcess.update_meta_file(date_list_new, meta_key, s3_bucket_conn)
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


class TestReturnDateList:
    """
    Test return date list
    """
    dates = [(datetime.today().date() - timedelta(days=day))
                 .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def test_return_date_list_no_meta_file(self, s3_bucket_conn):
        """
        Tests the return_date_list method
        when there is no meta file
        """
        # Expected results
        date_list_exp = [
            (datetime.today().date() - timedelta(days=day)) \
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)
        ]
        min_date_exp = (datetime.today().date() - timedelta(days=2)) \
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        # Test init
        first_date = min_date_exp
        meta_key = 'meta.csv'
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                         s3_bucket_conn)
        # Test after method execution
        assert set(date_list_exp) == set(date_list_return)
        assert min_date_exp == min_date_return

    def test_return_date_list_meta_file_ok(self, s3_meta_bucket, s3_bucket_conn):
        """
        Tests the return_date_list method
        when there is a meta file
        """
        # Expected results
        min_date_exp = [
            (datetime.today().date() - timedelta(days=1)) \
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (datetime.today().date() - timedelta(days=2)) \
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (datetime.today().date() - timedelta(days=7)) \
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]
        date_list_exp = [
            [(datetime.today().date() - timedelta(days=day)) \
                 .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(3)],
            [(datetime.today().date() - timedelta(days=day)) \
                 .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)],
            [(datetime.today().date() - timedelta(days=day)) \
                 .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(9)]
        ]
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}'
        )
        s3_meta_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
            self.dates[1],
            self.dates[4],
            self.dates[7]
        ]
        # Method execution
        for count, first_date in enumerate(first_date_list):
            min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                             s3_bucket_conn)
            # Test after method execution
            assert set(date_list_exp[count]) == set(date_list_return)
            assert min_date_exp[count] == min_date_return
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_meta_file_wrong(self, s3_meta_bucket, s3_bucket_conn):
        """
        Tests the return_date_list method
        when there is a wrong meta file
        """
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}'
        )
        s3_meta_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]
        # Method execution
        with pytest.raises(KeyError):
            MetaProcess.return_date_list(first_date, meta_key, s3_bucket_conn)
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_empty_date_list(self, s3_meta_bucket, s3_bucket_conn):
        """
        Tests the return_date_list method
        when there are no dates to be returned
        """
        # Expected results
        min_date_exp = '2200-01-01'
        date_list_exp = []
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[0]},{self.dates[0]}\n'
            f'{self.dates[1]},{self.dates[0]}'
        )
        s3_meta_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                         s3_bucket_conn)
        # Test after method execution
        assert date_list_exp == date_list_return
        assert min_date_exp == min_date_return
        # Cleanup after test
        s3_meta_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
