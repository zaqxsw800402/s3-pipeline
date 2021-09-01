from enum import Enum


class S3FileType(Enum):
    CSV = 'csv'
    PARQUET = 'parquet'


class MeteProcessFormat(Enum):
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
