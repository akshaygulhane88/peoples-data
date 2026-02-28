from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import logging
import traceback
from typing import Tuple
import re
from pyspark.sql import Row

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_struct(dtype: str) -> bool:
    '''
    Returns boolean value after checking whether the value's datatype is a struct
    :param dtype: datatype of value
    :return boolean true or false
    '''
    return True if dtype.startswith("struct") else False

def is_array(dtype: str) -> bool:
    '''
    Returns boolean value after checking whether the value's datatype is an array
    :param dtype: datatype of value
    :return boolean true or false
    '''
    return True if dtype.startswith("array") else False

def is_map(dtype: str) -> bool:
    '''
    Returns boolean value after checking whether the value's datatype is a map
    :param dtype: datatype of value
    :return boolean true or false
    '''
    return True if dtype.startswith("map") else False

def is_array_or_map(dtype: str) -> bool:
    '''
    Returns boolean value after checking whether the value's datatype is a map or an array
    :param dtype: datatype of value
    :return boolean true or false
    '''
    return True if (dtype.startswith("array") or dtype.startswith("map")) else False


def executesqlstatement(spark, sfoptions, query):
    '''
    Execute any snowflake query
    :param sfoptions: snowflake database options
    :param query: Merge query to execute
    :return status: None
    '''
    try:
        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfoptions, query)

    except Exception as e:
        logger.error("Unable to execute merge query" + ".\n{}".format(traceback.format_exc()))
        raise e


def createsparktempviewfromquery(spark, snowflake_source_name,sfoptions, query, sparktmpvw,docache=False):
    '''
    create spark temp views
    :param snowflake_source_name: snowflake connector source name
    :param sfoptions: snowflake database options
    :param query: query to execute
    :param sparktmpvw: spark temporary view name
    :param docache: cache spark temporary view if docache=True
    :return None
    '''
    try:
        df = spark.read.format(snowflake_source_name) \
            .options(**sfoptions) \
            .option("query", query) \
            .load()

        df.createOrReplaceTempView(sparktmpvw)
        
        if (docache==True):
            spark.catalog.cacheTable(sparktmpvw)

    except Exception as e:
        logger.error("Unable to create spark temporary view: " + sparktmpvw + ".\n{}".format(traceback.format_exc()))
        raise e


def createsparktempviewfromtable(spark, snowflake_source_name,sfoptions, dbtable, sparktmpvw,docache=False):
    '''
    create spark temp views
    :param snowflake_source_name: snowflake connector source name
    :param sfoptions: snowflake database options
    :param dbtable: table to be used
    :param sparktmpvw: spark temporary view name
    :param docache: cache spark temporary view if docache=True
    :return None
    '''
    try:
        df = spark.read.format(snowflake_source_name) \
            .options(**sfoptions) \
            .option("dbtable", dbtable) \
            .load()

        df.createOrReplaceTempView(sparktmpvw)
        
        if (docache==True):
            spark.catalog.cacheTable(sparktmpvw)

    except Exception as e:
        logger.error("Unable to create spark temporary view: " + sparktmpvw + ".\n{}".format(traceback.format_exc()))
        raise e



def getdffromsparksqlqry(spark, spsqlopts, query):
    '''
    Get a spark dataframe from spark sql query
    :param spsqlopts: list of spark sql options
    :param query: spark sql query to execute
    :return df: dataframe
    '''
    try:
        for option in spsqlopts:
            spark.sql(option)

        df = spark.sql(query)

        return df

    except Exception as e:
        logger.error("Unable to execute spark sql query" + ".\n{}".format(traceback.format_exc()))
        raise e


def loaddftotable(df, snowflakesourcename, sfoptions, schema, table, writemode):
    '''
    Load spark dataframe to snowflake table
    :param df: dataframe to be loaded ino snowflake table
    :param snowflakesourcename: snowflake source name
    :param sfoptions: snowflake db options
    :param schema: snowflake db schema
    :param table: snowflake table
    :param writemode: write mode
    :return None
    '''
    try:
        df.write.format(snowflakesourcename) \
            .options(**sfoptions) \
            .option("dbtable", table) \
            .mode(writemode).save()

    except Exception as e:
        logger.error("Unable to load snowflake table" + ".\n{}".format(traceback.format_exc()))
        raise e


def createdualtable(spark):
    '''
    create dual table
    :param spark: spark session object
    :return None
    '''
    try:
        df_dual = spark.sparkContext.parallelize([Row(r=Row("dummy"))]).toDF()
        df_dual.registerTempTable("dual")   

    except Exception as e:
        logger.error("Unable to create dual table" + ".\n{}".format(traceback.format_exc()))
        raise e

def getsha1changehash(df:DataFrame,columns:list)->DataFrame:
    '''
    Calculate and return sha1 value of concatenated columns
    :param df: Pyspark Dataframe 
    :param columns: List of columns to concatenate 
    :return Pyspark dataframe with a new column change_hash
    '''
    try:
        newdf = df.withColumn("change_hash", sha1(concat_ws("|~|", *columns)))
        return newdf
        
    except Exception as e:
        logger.error("Unable to compute sha1 change hash" + ".\n{}".format(traceback.format_exc()))
        raise e
    
        

def diff_dataframes(new_df: DataFrame, existing_df: DataFrame, reference_col: str) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    '''
    Author: AWS team
    diff_dataframes will compare two dataframes and return four dataframes containing new records, deleted records, modified records, and unmodified records.
    :param new_df: incoming pyspark dataframe to compare
    :param existing_df: original existing pyspark dataframe to compare
    :param reference_col: column that acts as a unique identifier
    :return four pyspark dataframes
    :raise None
    '''
    try:
        new_and_modified_records = new_df.subtract(existing_df)
        modified_records = new_and_modified_records.join(
            existing_df, how="leftsemi", on=reference_col
        )
        new_records = new_and_modified_records.join(
            existing_df, how="leftanti", on=reference_col
        )
        deleted_records = existing_df.subtract(new_df).join(
            modified_records, how="leftanti", on=reference_col
        )
        unmodified_records = new_df.intersect(existing_df)

        return new_records, deleted_records, modified_records, unmodified_records
        
    except Exception as e:
        logger.error("Dataframe diff fails due to the error" + ".\n{}".format(traceback.format_exc()))
        raise e
    
        
def getsparkdffromquery(spark, snowflake_source_name,sfoptions, query, autopushdown="off"):
    '''
    Get spark dataframe created from snowflake query
    :param snowflake_source_name: snowflake connector source name
    :param sfoptions: snowflake database options
    :param query: query to execute
    :return dataframe
    '''
    try:
        df = spark.read.format(snowflake_source_name) \
            .options(**sfoptions) \
            .option("query", query) \
            .option("autopushdown", autopushdown) \
            .load()

        return df

    except Exception as e:
        logger.error("Unable to create spark dataframe from query: " + ".\n{}".format(traceback.format_exc()))
        raise e


def getsparkdffromtable(spark, snowflake_source_name,sfoptions, dbtable, autopushdown="off"):
    '''
    Get spark dataframe created from snowflake table
    :param snowflake_source_name: snowflake connector source name
    :param sfoptions: snowflake database options
    :param dbtable: table to be used
    :return dataframe
    '''
    try:
        df = spark.read.format(snowflake_source_name) \
            .options(**sfoptions) \
            .option("dbtable", dbtable) \
            .option("autopushdown", autopushdown) \
            .load()
            
        return df

    except Exception as e:
        logger.error("Unable to create spark dataframe from table: " + ".\n{}".format(traceback.format_exc()))
        raise e
    
        
def renameCols(df, src_cols, tgt_cols):
    '''
    Rename source dataframe columns with target dataframe columns
    :param df: spark dataframe
    :param src_cols: source columns list
    :param tgt_cols: target columns list
    :return modified dataframe
    '''
    try:
        for src_col,tgt_col in zip(src_cols,tgt_cols):
            df = df.withColumnRenamed(src_col,tgt_col)
        return df
    
    except Exception as e:
        logger.error("Dataframe columns renaming fails due to the error" + ".\n{}".format(traceback.format_exc()))
        raise e
        
        
def convert_decimal_to_intordouble(colType):
    '''
    Checks and converts decimal datatype to long or double datatype
    :param colType: column datatype
    :return 'long' or 'double'
    '''
    try:
        [digits, decimals] = re.findall(r'\d+', colType)
        # if there's no decimal points, convert it to int
        return 'long' if decimals == '0' else 'double'
        
    except Exception as e:
        logger.error("Dataframe columns decimal datatype check fails due to the error" + ".\n{}".format(traceback.format_exc()))
        raise e    
        
        
def create_nested_struct_spark_df(spark,tmpVw,structCols,grpCols,structAlias):
    '''
    Creates a nested spark dataframe
    :param spark: spark session
    :param tmpVw: spark temporary view to select data from
    :param structCols: Columns to be created in a struct
    :param grpCols: columns to be used in group by clause
    :return: nested spark dataframe
    '''
    try:
        sql = "SELECT " +  ",".join(grpCols) + ",COLLECT_LIST(STRUCT(" + ",".join(structCols) + ")) AS "+ structAlias +" FROM " +  tmpVw + " GROUP BY " + ",".join(grpCols)
        nestedsdf = spark.sql(sql)
        return nestedsdf
        
    except Exception as e:
        logger.error("Dataframe columns decimal datatype check fails due to the error" + ".\n{}".format(traceback.format_exc()))
        raise e 
    