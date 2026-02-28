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


class Rdbms():
    def __init__(self):
        self.reader = None

    def get_dataframe_from_snowflake(self, spark, snowflake_source_name, sfoptions, query):
        '''
        :param spark: spark client object
        :param snowflake_source_name: snowflake jdbc class name
        :param sfoptions: Snowflake connection options
        :param options: spark read options as a dictionary
        :return spark dataframe
        '''
        try:
            df = spark.read.format(snowflake_source_name).options(**sfoptions).option("query", query).load()
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e

    def get_dataframe_from_rdbms(self, spark, db_driver, db_url, db_query, db_user_name, db_password):
        '''
        :param db_driver: DB driver class
        :param db_url: jdbc url
        :param db_query: query to be executed in database
        :param db_user_name: database user name
        :param db_password: database password
        :return spark dataframe
        '''
        try:
            df = spark.read \
                .format("jdbc") \
                .option("driver", db_driver) \
                .option("url", db_url) \
                .option("query", db_query) \
                .option("user", db_user_name) \
                .option("password", db_password).load()
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e

    def get_partitioned_dataframe_from_oracle(self, spark, db_driver, db_url, db_query, db_user_name, db_password,
                                             partition_info):
        '''
        :param db_driver: DB driver class
        :param db_url: jdbc url
        :param db_query: query to be executed in database
        :param db_user_name: database user name
        :param db_password: database password
        :param partition_info: partition information such as partition_column, partition_query, num_partitions in a dictionary
        :return spark dataframe
        '''
        try:
            partition_query = partition_info["partition_query"]
            part_col_datatype = partition_info["part_col_datatype"]
            minmaxdf = spark.read \
                .format("jdbc") \
                .option("driver", db_driver) \
                .option("url", db_url) \
                .option("query", partition_query) \
                .option("user", db_user_name) \
                .option("password", db_password).load()

            partition_column = partition_info["partition_column"]
            num_partitions = partition_info["num_partitions"]
            fetch_size = partition_info["fetch_size"]
            
            if part_col_datatype=="int":
                min_value = int(minmaxdf.collect()[0][0])
                max_value = int(minmaxdf.collect()[0][1])
                print("min_value is :",min_value)
                print("max value is :",max_value)

                df = spark.read \
                .format("jdbc") \
                .option("driver", db_driver) \
                .option("url", db_url) \
                .option("dbtable", db_query) \
                .option("user", db_user_name) \
                .option("password", db_password) \
                .option("partitionColumn", partition_column) \
                .option("lowerBound", min_value) \
                .option("upperBound", max_value) \
                .option("numPartitions", num_partitions) \
                .option("fetchsize", fetch_size).load()
                
            else:
                min_value = minmaxdf.collect()[0][0]
                max_value = minmaxdf.collect()[0][1]
                
                print("min_value is :",min_value)
                print("max value is :",max_value)
                
                session_stmt = partition_info["session_stmt"]
                df = spark.read \
                    .format("jdbc") \
                    .option("driver", db_driver) \
                    .option("url", db_url) \
                    .option("dbtable", db_query) \
                    .option("user", db_user_name) \
                    .option("password", db_password) \
                    .option("partitionColumn", partition_column) \
                    .option("lowerBound", min_value) \
                    .option("upperBound", max_value) \
                    .option("numPartitions", num_partitions) \
                    .option("sessionInitStatement", session_stmt) \
                    .option("oracle.jdbc.mapDateToTimestamp", "false") \
                    .option("fetchsize", fetch_size).load()
            
            return df

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e
