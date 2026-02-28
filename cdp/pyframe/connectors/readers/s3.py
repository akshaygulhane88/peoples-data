import requests
import traceback
import logging
import json
import gzip
from io import BytesIO
from io import TextIOWrapper
from gzip import GzipFile
import pandas as pd
import io

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3:
    def __init__(self):
        self.reader = None

    def get_dataframe_from_filesources(self, spark, fileformat, paths, options):
        '''
        :param spark: spark client object
        :param fileformat: file format - for eg:json
        :param options: spark read options as a dictionary
        :param paths: S3 paths
        :return spark dataframe
        '''
        try:
            df = spark.read.format(fileformat).options(**options).load(paths)
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e

    def get_dataframe_from_gzip_multiline_json_filesources(self, s3, spark, paths):
        '''
        Get dataframe from gzipped multiline json filesources
        :param s3: s3 client object
        :param spark: spark client object
        :param paths: S3 paths
        :return spark dataframe
        '''
        try:
            paths_json_list = []
            json_files_list = []
            for path in paths:
                bucket, key = path.replace("s3://", "").split("/", 1)
                if path.endswith('.json.gz'):
                    contents = s3.get_object(Bucket=bucket, Key=key)
                    gzipped = GzipFile(None, 'rb', fileobj=contents['Body'])
                    data = TextIOWrapper(gzipped)
                    json_list = [json.loads(line) for line in data]
                    paths_json_list.extend(json_list)
                else:
                    for obj in s3.list_objects_v2(Bucket=bucket, Prefix=key)['Contents']:
                        s3_json_file = "s3://" + bucket + "/" + obj['Key']
                        json_files_list.append(s3_json_file)
                    for s3_json_file in json_files_list:
                        bucket, key = s3_json_file.replace("s3://", "").split("/", 1)
                        contents = s3.get_object(Bucket=bucket, Key=key)
                        gzipped = GzipFile(None, 'rb', fileobj=contents['Body'])
                        data = TextIOWrapper(gzipped)
                        json_list = [json.loads(line) for line in data]
                        paths_json_list.extend(json_list)
            jsonrdd = spark.sparkContext.parallelize(paths_json_list).map(lambda x: json.dumps(x))
            df = spark.read.option("inferSchema", "true").json(jsonrdd)
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e


    def get_dataframe_from_multiline_json_filesources(self, s3, spark, paths):
        '''
        Get dataframe from multiline json filesources
        :param s3: s3 client object
        :param spark: spark client object
        :param paths: S3 paths
        :return spark dataframe
        '''
        try:
            paths_json_list = []
            json_files_list = []
            for path in paths:
                bucket, key = path.replace("s3://", "").split("/", 1)
                if path.endswith('.json'):
                    data = s3.get_object(Bucket=bucket, Key=key)['Body'].iter_lines()
                    json_list = [json.loads(line) for line in data]
                    paths_json_list.extend(json_list)
                else:
                    for obj in s3.list_objects_v2(Bucket=bucket, Prefix=key)['Contents']:
                        s3_json_file = "s3://" + bucket + "/" + obj['Key']
                        json_files_list.append(s3_json_file)
                    for s3_json_file in json_files_list:
                        bucket, key = s3_json_file.replace("s3://", "").split("/", 1)
                        data = s3.get_object(Bucket=bucket, Key=key)['Body'].iter_lines()
                        json_list = [json.loads(line) for line in data]
                        paths_json_list.extend(json_list)
            jsonrdd = spark.sparkContext.parallelize(paths_json_list).map(lambda x: json.dumps(x))
            df = spark.read.option("inferSchema", "true").json(jsonrdd)
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e
            
            
            
    def read_prefix_parquet_to_df(self, s3res, bucket, keyprefix):
        '''
        Create pandas dataframe from parquet snappy compressed files in a S3 bucket folder
        :param s3res: S3 resource object
        :param bucket: S3 bucket name
        :param keyprefix: Key prefix which contains the parquet snappy compressed files
        :return df: pandas dataframe
        '''
        try:
            bucket = s3res.Bucket(bucket)
            prefix_objs = bucket.objects.filter(Prefix=keyprefix)
            prefix_df = []
            for obj in prefix_objs:
                key = obj.key
                body = obj.get()['Body'].read()
                df = pd.read_parquet(io.BytesIO(body))   
                prefix_df.append(df)
            return pd.concat(prefix_df)
            
        except Exception as e:
            logger.error("Unable to create pandas dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e
