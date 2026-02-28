import sys
import base64
import boto3
import json
import pyspark.sql
import logging
import traceback
from awsglue.job import Job
from pyspark.sql.types import *
from awsglue.transforms import *
import pyspark.sql.functions  as F
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from pyspark.context import SparkContext, SparkConf
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from py4j.java_gateway import java_import
from datetime import datetime
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from cdp.pyframe.configparser import configparser
from cdp.pyframe.connectors.readers import rdbms
from cdp.pyframe.connectors.writers import s3 as s3w
from cdp.pyframe.utils import secretretriever,pythonfuncs as pf,pysparkfuncs as psf
from cdp.pyframe import constants
import numpy as np
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'table','source_system','config_bucket','config_key','environment']) #Changes made on 22/06/2022 as part of refactoring
    
## @params: [JOB_NAME]
# 1. Create spark session, glue context, initialize job and get job parameters
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config_bucket = args['config_bucket']
config_key = args['config_key']
environment = args['environment'] #Changes made on 22/06/2022 as part of refactoring

# 2. Initialize logger
logger =  glueContext.get_logger()
logger.info("Start of RDBMS ingestion")

# 3. Create S3 client object and get the config object
s3 = boto3.resource('s3')
s3_conn = boto3.client(constants.S3)

try:
    configparser = configparser.ConfigParser()
    jconfig = configparser.getjconfigobject(s3_conn,config_bucket,config_key)
    logger.info("Config file is parsed and Json config object is created successfully")
    
except Exception as e:
    print("Unable to parse s3 config object" + ".\n{}".format(traceback.format_exc()))
    raise e

#capture table dictionary key values for source and target
envparams = configparser.getenvironmentparameters(jconfig)[environment] #Changes made on 22/06/2022 as part of refactoring
dynamic_vars = envparams['dynamic_vars'] #Changes made on 22/06/2022 as part of refactoring
src2tgtparams = configparser.getsectionparameters(jconfig,constants.SRC2TGT,dynamic_vars) #Changes made on 22/06/2022 as part of refactoring
sourceparams = src2tgtparams['sources'][args['source_system']][args['table']] #Changes made on 22/06/2022 as part of refactoring
targetparams = src2tgtparams['targets']['s3'][args['table']] #Changes made on 22/06/2022 as part of refactoring
controlparams = configparser.getcontrolparameters(jconfig,dynamic_vars)[args['table']] #Changes made on 22/06/2022 as part of refactoring

#collect table level details
db_url = sourceparams['jdbcUrl']
db_driver_path = sourceparams['jdbcdriverpath']
db_driver = sourceparams['connectionProperties']['driver']
sourcequery = sourceparams['sourcequery']
source_load_type = sourceparams['load_type']
partition_flag = sourceparams.get('partition_flag','N')

#store target details
target_bucket_name = targetparams['target_bucket_name']
target_key_name = targetparams['target_key_name']
target_file_format = targetparams['target_file_format']
target_write_options = targetparams['writer_spark_options']
partition_columns = targetparams['target_partition_by']
target_write_mode = targetparams['target_write_mode']

# Get control params
sfconnection = controlparams['connection']
sfctrldatabase = controlparams['connection']['sfdatabase']
sfctrlschema = controlparams['schema']
sfctrltable = controlparams['name']
dfquery = controlparams['dfquery']
procquery_raw = controlparams['procquery']
snowflake_source_name = controlparams['snowflake_source_name']

logger.info("source params are :{}".format(' '.join(map(str, sourceparams))))
logger.info("target params are :{}".format(' '.join(map(str, targetparams))))
logger.info("control params are :{}".format(' '.join(map(str,controlparams))))  
logger.info("environment params are :{}".format(' '.join(map(str,envparams)))) 

#Retrieving snowflake user and password from Snowflake secrets
snowflake_db_secret_name = envparams['snowflake_db_secret_name'] #Changes made on 22/06/2022 as part of refactoring
sfsecretobj = secretretriever.Secret() #Changes made on 22/06/2022 as part of refactoring
sf_secret= json.loads(sfsecretobj.get_secret(snowflake_db_secret_name)) #Changes made on 22/06/2022 as part of refactoring
sfuser = sf_secret['username'] #Changes made on 22/06/2022 as part of refactoring
sfPassword = sf_secret['password'] #Changes made on 22/06/2022 as part of refactoring
sfurl = sf_secret['url'] #Changes made on 22/06/2022 as part of refactoring
sfaccount = sf_secret['account'] #Changes made on 22/06/2022 as part of refactoring
sfwarehouse = sf_secret['warehouse'] #Changes made on 22/06/2022 as part of refactoring

sfconnection['sfusername']=sfuser
sfconnection['sfpassword']=sfPassword
sfconnection['sfurl'] = sfurl #Changes made on 22/06/2022 as part of refactoring
sfconnection['sfaccount'] = sfaccount #Changes made on 22/06/2022 as part of refactoring
sfconnection['sfwarehouse'] = sfwarehouse #Changes made on 22/06/2022 as part of refactoring



#Get user name and password from RDBMS secrets    
db_secret_name = sourceparams['connectionProperties']['secretname'] #Changes made on 22/06/2022 as part of refactoring
print('Secret name : ', db_secret_name)
dbsecretobj = secretretriever.Secret() #Changes made on 22/06/2022 as part of refactoring
db_secret= json.loads(dbsecretobj.get_secret(db_secret_name)) #Changes made on 22/06/2022 as part of refactoring
db_user_name =  db_secret['username'] #Changes made on 22/06/2022 as part of refactoring
#print('DB user name', db_user_name)
db_password = db_secret['password'] #Changes made on 22/06/2022 as part of refactoring
#print('DB password :', db_password)


# Get pandas dataframe of control table
controlpdf = pf.getpandasdffromsnowflake(sfconnection,dfquery)
# print("controlpdf is:",controlpdf.head())

start_timestamp = np.datetime_as_string(controlpdf['START_TIMESTAMP'].values[0])
end_timestamp = np.datetime_as_string(controlpdf['END_TIMESTAMP'].values[0])
print('this is start', start_timestamp)
print('this is start', end_timestamp)

logger.info(start_timestamp)
logger.info(end_timestamp)

#4. collect custom query for table
db_query = pf.get_query_format(sourcequery,source_load_type,end_timestamp)
logger.info("{}".format(db_query))

# Get current timestamp
current_timestamp = datetime.now()
current_timestamp_str = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")


#6. read read source db with query
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY")

rdbmsobj = rdbms.Rdbms() #Changes made on 22/06/2022 as part of refactoring
print("RDBMS object :", rdbmsobj)

#Partiton implimentation 

if partition_flag.lower()=='y': 
	rdbms_table_sdf = rdbmsobj.get_partitioned_dataframe_from_oracle(spark,db_driver,db_url,db_query,db_user_name,db_password,sourceparams['partition_info'])
	print("Using get_partitioned_dataframe_from_rdbms")
else: 
	rdbms_table_sdf = rdbmsobj.get_dataframe_from_rdbms(spark,db_driver,db_url,db_query,db_user_name,db_password) #Changes made on 22/06/2022 as part of refactoring
	print("Using get_dataframe_from_rdbms")
#rdbms_table_sdf.show()

#rdbms_table_fmt_sdf = rdbms_table_sdf.withColumn('date',F.lit(current_date)).withColumn('hour',F.lit(current_hour)) #Changes made on 22/06/2022 as part of refactoring
#logger.info('this is rdbms_table_fmt_sdf :{}'.format(rdbms_table_fmt_sdf))

logger.info('this is the file count')
#print(rdbms_table_fmt_sdf.count())

#7.store to s3 path
target_s3_path = pf.get_target_s3_path(source_load_type,target_bucket_name,target_key_name) #Changes made on 22/06/2022 as part of refactoring

#8. write to s3 path with config_file file format

s3wobj =  s3w.S3()
s3wobj.write_dataframe_to_S3(rdbms_table_sdf,target_file_format,target_s3_path,target_write_mode,target_write_options) #Changes made on 22/06/2022 as part of refactoring

    
# 9. Update Proc start and end timestamp query
procendtsquery = procquery_raw.format("'END_TIMESTAMP'","'''" + current_timestamp_str +"'''")
print("proc end ts query is",procendtsquery)
pf.execprocsinsnowflake(sfconnection,procendtsquery)

procstarttsquery = procquery_raw.format("'START_TIMESTAMP'","'END_TIMESTAMP'")
print("proc start ts query is",procstarttsquery)
pf.execprocsinsnowflake(sfconnection,procstarttsquery)
    
job.commit()