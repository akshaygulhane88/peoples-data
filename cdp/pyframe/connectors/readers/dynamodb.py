import requests
import traceback
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Dynamodb():
    def __init__(self):
        self.reader = None


    def get_dataframe_from_dynamodb(self, spark, dyn_format, options):
        '''
        :param spark: spark client object
        :param dynamodb format: dynamodb format class name
        :param options: Spark options as a dictionary
        :return spark dataframe
        '''
        try:
            df = spark.read.format(dyn_format).options(**options).load()
            return df
    
        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e
        
            