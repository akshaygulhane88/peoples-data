import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import json
import boto3
from py4j.java_gateway import java_import
from pyspark import SparkContext
from cdp.pyframe.configparser import configparser
from cdp.pyframe import constants
from cdp.pyframe.utils import pythonfuncs as pf
from cdp.pyframe.utils import secretretriever
from cdp.pyframe.utils import pysparkfuncs as psf
import traceback
import numpy as np


# 1. Get command line arguments and create spark, glue context and spark session
# Changes made on 06/06/2022 for handling enviroment parameters related changes
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_bucket','config_key','environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize logger
logger =  glueContext.get_logger()
logger.info("Spark and glue session objects are created successfully")

config_bucket = args['config_bucket']
config_key = args['config_key']

# Changes made on 06/06/2022 for handling enviroment parameters related changes
environment = args['environment']

# 2. Create S3 and snowflake connection objects and get the config object
s3_conn = boto3.client(constants.S3)

try:
    config_obj = configparser.ConfigParser()
    jconfig = config_obj.getjconfigobject(s3_conn,config_bucket,config_key)
    logger.info("Config file is parsed and Json config object is created successfully")
except Exception as e:
    logger.error("Unable to parse s3 config object" + ".\n{}".format(traceback.format_exc()))
    raise e

global_params = config_obj.getglobalparameters(jconfig)
env_params = config_obj.getenvironmentparameters(jconfig)[environment]
dynamic_vars = env_params['dynamic_vars']
job_params = config_obj.getjobparameters(jconfig)
stage_params = config_obj.getstagesparameters(jconfig,dynamic_vars)
controlparams = config_obj.getcontrolparameters(jconfig,dynamic_vars)

snowflake_source_name = global_params['snowflake_source_name']

print("global_params are :", global_params)
print("env_params are :", env_params)
print("job_params are :", job_params)
print("stage_params are :", stage_params)
print("control params are :", controlparams)

logger.info("Environment, Job and stages parameters are created successfully")


# Stage: stg_tables_stg_to_temp params
stg_to_temp_src_params = stage_params[0]['stg_tables_stg_to_temp']['sources'][0]
stg_to_temp_tgt_params = stage_params[0]['stg_tables_stg_to_temp']['targets'][0]

# Stage: merge_temp_table_to_edw params
temp_table_to_edw_src_params = stage_params[1]['merge_temp_table_to_edw']['sources'][0]
temp_table_to_edw_tgt_params = stage_params[1]['merge_temp_table_to_edw']['targets'][0]

# '''Reusable

# Get control params
sfconnection = controlparams['connection']
sfctrldatabase = controlparams['connection']['sfdatabase']
sfctrlschema = controlparams['schema']
sfctrltable = controlparams['name']
dfquery = controlparams['dfquery']
snowflake_source_name = controlparams['snowflake_source_name']
procquery_raw = controlparams['procquery']
print("Proc update query is",procquery_raw)

# Snowflake connection object preparation
snowflake_db_secret_name = env_params['snowflake_db_secret_name']
secretobj = secretretriever.Secret()
secret= json.loads(secretobj.get_secret(snowflake_db_secret_name))

sfuser = secret['username']
sfPassword = secret['password']
sfurl = secret['url']
sfaccount = secret['account']
sfwarehouse = secret['warehouse']

sfconnection['sfusername']=sfuser
sfconnection['sfpassword']=sfPassword

# Changes made on 06/06/2022 for handling enviroment parameters related changes
sfconnection['sfurl'] = sfurl # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfconnection['sfaccount'] = sfaccount # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfconnection['sfwarehouse'] = sfwarehouse # Changes made on 06/06/2022 for handling enviroment parameters related changes

java_import(spark._jvm, snowflake_source_name)

spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())


# Reusable'''


sfstgurl = secret['url'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfstgaccount = secret['account'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfstgwarehouse = secret['warehouse'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfstgschema = stg_to_temp_src_params['schema']
sfstgrole = env_params['role']
sfstgdatabase = env_params['database']

sfOptionsstg = pf.getsnowflakeconobj(sfstgurl,sfstgaccount,sfuser,sfPassword,sfstgwarehouse,sfstgdatabase,sfstgschema,sfstgrole)


sftempurl = secret['url'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sftempaccount = secret['account'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sftempwarehouse = secret['warehouse'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sftempschema = stg_to_temp_tgt_params['schema']
sftemprole = env_params['role']
sftempdatabase = env_params['database']

sfOptionstemp = pf.getsnowflakeconobj(sftempurl,sftempaccount,sfuser,sfPassword,sftempwarehouse,sftempdatabase,sftempschema,sftemprole)


sfdataurl = secret['url'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfdataaccount = secret['account'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfdatawarehouse = secret['warehouse'] # Changes made on 06/06/2022 for handling enviroment parameters related changes
sfdataschema = temp_table_to_edw_tgt_params['schema']
sfdatarole = env_params['role']
sfdatadatabase = env_params['database']

sfOptionsdata = pf.getsnowflakeconobj(sfdataurl,sfdataaccount,sfuser,sfPassword,sfdatawarehouse,sfdatadatabase,sfdataschema,sfdatarole)

logger.info("Snowflake connection objects are created successfully")

#-----------------------------------------------------------------------------------------------------------------------------------------------
# Get pandas dataframe of control table
# Get control params

controlpdf = pf.getpandasdffromsnowflake(sfconnection,dfquery)
# print("controlpdf is:",controlpdf.head())

start_timestamp = controlpdf['START_TIMESTAMP'].values[0]
end_timestamp = controlpdf['END_TIMESTAMP'].values[0]

start_timestamp_str = np.datetime_as_string(controlpdf['START_TIMESTAMP'].values[0])
end_timestamp_str = np.datetime_as_string(controlpdf['END_TIMESTAMP'].values[0])

print("start timestamp is :",start_timestamp)
print("End timestamp is :",end_timestamp)

#----------------------------------------------------------------------------------------------------------------------------------------------

# Update control table by calling stored procedure
# Proc start timestamp query
procstarttsquery = procquery_raw.format("'END_TIMESTAMP'","'CURRENT_TIMESTAMP()'")
print("proc start ts query is",procstarttsquery)
pf.execprocsinsnowflake(sfconnection,procstarttsquery)

#----------------------------------------------------------------------------------------------------------------------------------------------
# Update CURRENT_RECORD_INDICATOR

updatedatastmt1 = """
update {0}.{1}.{2}
set CURRENT_RECORD_INDICATOR='Y'
WHERE CURRENT_TIMESTAMP  BETWEEN START_DATE AND nvl(END_DATE,to_date('31-12-4712','dd-MM-yyyy')) AND CURRENT_RECORD_INDICATOR='N'
""".format(sfOptionsdata['sfDatabase'],sfOptionsdata['sfSchema'], temp_table_to_edw_tgt_params['name'])

print("Current Record statement:1 (updatedatastmt1) is:",updatedatastmt1)
psf.executesqlstatement(spark,sfOptionsdata,updatedatastmt1)

logger.info("Current Record statement:1 is executed sucessfully")

updatedatastmt2 = """
update {0}.{1}.{2}
set CURRENT_RECORD_INDICATOR='N'
WHERE (CURRENT_TIMESTAMP NOT BETWEEN START_DATE AND nvl(END_DATE,to_date('31-12-4712','dd-MM-yyyy')) AND CURRENT_RECORD_INDICATOR='Y') 
""".format(sfOptionsdata['sfDatabase'],sfOptionsdata['sfSchema'], temp_table_to_edw_tgt_params['name'])

print("Current Record statement:2 (updatedatastmt2) is:",updatedatastmt2)
psf.executesqlstatement(spark,sfOptionsdata,updatedatastmt2)

logger.info("Current Record statement:2 is executed sucessfully")

logger.info('DATA.ED_PERSON_EXPERTISE_TOPIC_D current record successfully updated')

# Apply business transformation logic and create  temporary stage

pastgquery = stg_to_temp_src_params['query']
pastgtmpvw = stg_to_temp_src_params['spark_temp_vw_name']
print("Stg table query (pastgquery) is:",pastgquery)
psf.createsparktempviewfromquery(spark,snowflake_source_name,sfOptionsstg,pastgquery,pastgtmpvw)

logger.info("Spark temporary view "+ pastgtmpvw +" is created successfully")

query = """SELECT   FMNO, 
                    TOPIC_ID, 
                    TOPIC_NAME, 
                    TOPIC_DESCRIPTION, 
                    TOPIC_TAXONOMY_URL_ID, 
                    TAXONOMY_TAG_VALUE, 
                    TOPIC_TAXNOMY_TYPE, 
                    TOPIC_PRACTICE_URL_ID,
                    PRACTICE_RECOMMENDED ,
                    FIRST_ALERT ,
                    START_DATE, 
                    END_DATE, 
                    CREATION_DATE, 
                    UPDATE_DATE,
                    CASE
                       WHEN CURRENT_TIMESTAMP BETWEEN START_DATE AND nvl(END_DATE,to_date('31-12-4712','dd-MM-yyyy')) THEN 'Y'
                       ELSE 'N'
                       END                                               CURRENT_RECORD_INDICATOR
            FROM {0}
""".format(pastgtmpvw)

spksqlopts = job_params['spark_options']

print("query (pa_temp_stg_sdf) is:",query)

pa_temp_stg_sdf = psf.getdffromsparksqlqry(spark,spksqlopts,query)

print(pa_temp_stg_sdf)

logger.info("Spark temporary dataframe pa_temp_stg_sdf is created successfully")

print("sfDatabase is :",sfOptionstemp['sfDatabase'])
print("sfSchema is :",sfOptionstemp['sfSchema'])
print("table is :",temp_table_to_edw_src_params['table'])

# Create the temporary table on top of transformed data

temptblddlstmt= """
    CREATE OR REPLACE TEMPORARY TABLE {0} (
	FMNO VARCHAR,
	TOPIC_ID NUMBER,
	TOPIC_NAME VARCHAR,
	TOPIC_DESCRIPTION VARCHAR,
	TOPIC_TAXONOMY_URL_ID VARCHAR,
	TAXONOMY_TAG_VALUE VARCHAR,
	TOPIC_TAXNOMY_TYPE VARCHAR, 
	TOPIC_PRACTICE_URL_ID VARCHAR, 
    PRACTICE_RECOMMENDED VARCHAR,
    FIRST_ALERT VARCHAR,
	START_DATE TIMESTAMPNTZ,
	END_DATE TIMESTAMPNTZ,
	CREATION_DATE TIMESTAMPNTZ,
	UPDATE_DATE	TIMESTAMPNTZ,
    CURRENT_RECORD_INDICATOR VARCHAR
    );
    """.format(temp_table_to_edw_src_params['table'])

print("Temp table DDL (temptblddlstmt) is:",temptblddlstmt)

psf.executesqlstatement(spark,sfOptionstemp,temptblddlstmt)

logger.info("Temporary table " + temp_table_to_edw_src_params['table'] + " is created successfully in TEMP schema")

# Write the transformed data into temporary staging table

schema = sfOptionstemp['sfSchema']
table = temp_table_to_edw_src_params['table']
write_mode = temp_table_to_edw_src_params['write_mode']
psf.loaddftotable(pa_temp_stg_sdf,snowflake_source_name,sfOptionstemp,schema,table,write_mode)


# 7. Create the data layer - Load the temp data into Snowflake Staging
# Stage: load_temp_table_to_edw_ --
'''This stage is used to merge data from data from leave type ref temp table in temp schema into table in EDW schema'''



mergetmpedwstmt = """
MERGE INTO {0}.{1}.{2} TGT 
 USING ( SELECT *, 
                CAST(SHA1( 
                          NVL(CAST(FMNO AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_ID AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_NAME AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_DESCRIPTION AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_TAXONOMY_URL_ID AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TAXONOMY_TAG_VALUE AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_TAXNOMY_TYPE AS VARCHAR),'')||'|~|'||
                           NVL(CAST(PRACTICE_RECOMMENDED AS VARCHAR),'')||'|~|'||
                           NVL(CAST(FIRST_ALERT AS VARCHAR),'')||'|~|'||
                           NVL(CAST(START_DATE AS VARCHAR),'')||'|~|'||
                           NVL(CAST(END_DATE AS VARCHAR),'')||'|~|'||
                           NVL(CAST(CURRENT_RECORD_INDICATOR AS VARCHAR),'')||'|~|'||
                           NVL(CAST(TOPIC_PRACTICE_URL_ID AS VARCHAR),'') ) AS CHAR(40)) CHANGE_HASH 
          FROM {3}.{4}.{5} ) SRC 
     ON TGT.FMNO = SRC.FMNO
     AND TGT.TOPIC_ID = SRC.TOPIC_ID
     AND TGT.TOPIC_NAME = SRC.TOPIC_NAME
     AND TGT.START_DATE = SRC.START_DATE
    AND TGT.TOPIC_TAXONOMY_URL_ID = SRC.TOPIC_TAXONOMY_URL_ID
 WHEN MATCHED AND TGT.CHANGE_HASH <> SRC.CHANGE_HASH THEN 
 UPDATE SET 
    TGT.TOPIC_DESCRIPTION = SRC.TOPIC_DESCRIPTION,
    TGT.TOPIC_TAXONOMY_URL_ID = SRC.TOPIC_TAXONOMY_URL_ID,
    TGT.TAXONOMY_TAG_VALUE = SRC.TAXONOMY_TAG_VALUE,
    TGT.TOPIC_TAXNOMY_TYPE = SRC.TOPIC_TAXNOMY_TYPE,
    TGT.TOPIC_PRACTICE_URL_ID = SRC.TOPIC_PRACTICE_URL_ID,
    TGT.PRACTICE_RECOMMENDED = SRC.PRACTICE_RECOMMENDED,
    TGT.FIRST_ALERT = SRC.FIRST_ALERT,
    TGT.END_DATE = SRC.END_DATE,
    TGT.UPDATE_DATE = CURRENT_TIMESTAMP(),
    TGT.CURRENT_RECORD_INDICATOR = SRC.CURRENT_RECORD_INDICATOR,
    TGT.CHANGE_HASH = SRC.CHANGE_HASH
WHEN NOT MATCHED THEN 
 INSERT ( 
     FMNO,
     TOPIC_ID,
     TOPIC_NAME,
     TOPIC_DESCRIPTION,
     TOPIC_TAXONOMY_URL_ID,
     TAXONOMY_TAG_VALUE,
     TOPIC_TAXNOMY_TYPE,
     TOPIC_PRACTICE_URL_ID,
     PRACTICE_RECOMMENDED,
     FIRST_ALERT,
     START_DATE,
     END_DATE,
     CURRENT_RECORD_INDICATOR,
     CREATION_DATE,
     UPDATE_DATE,
     CHANGE_HASH)
 VALUES (
     SRC.FMNO,
     SRC.TOPIC_ID,
     SRC.TOPIC_NAME,
     SRC.TOPIC_DESCRIPTION,
     SRC.TOPIC_TAXONOMY_URL_ID,
     SRC.TAXONOMY_TAG_VALUE,
     SRC.TOPIC_TAXNOMY_TYPE,
     SRC.TOPIC_PRACTICE_URL_ID,
     SRC.PRACTICE_RECOMMENDED,
     SRC.FIRST_ALERT,
     SRC.START_DATE,
     SRC.END_DATE,
     SRC.CURRENT_RECORD_INDICATOR,
     CURRENT_TIMESTAMP(),
     CURRENT_TIMESTAMP(),
     SRC.CHANGE_HASH);
""".format(sfOptionsdata['sfDatabase'],sfOptionsdata['sfSchema'], temp_table_to_edw_tgt_params['name'],
           sfOptionstemp['sfDatabase'], sfOptionstemp['sfSchema'], temp_table_to_edw_src_params['name'])

print("Insert statement for new rows from temp table to insert rows to EDW table (mergetmpedwstmt) is:",mergetmpedwstmt)
psf.executesqlstatement(spark,sfOptionsdata,mergetmpedwstmt)

logger.info("Merge and insert statements for merging temp table with EDW DATA table is executed sucessfully")

logger.info('DATA.ED_PERSON_EXPERTISE_TOPIC_D successfully udpated')


#-----------------------------------------------------------------------------------------------------------------------------------------------
# Update control table by calling stored procedure
# Proc end timestamp query
procendtsquery = procquery_raw.format("'START_TIMESTAMP'","'END_TIMESTAMP'")
print("proc end ts query is",procendtsquery)
pf.execprocsinsnowflake(sfconnection,procendtsquery)

#-----------------------------------------------------------------------------------------------------------------------------------------------



# Post-processing: Cleanup operation
droptempstmt = """DROP TABLE IF EXISTS {0}.{1}.{2};""".format(sfOptionstemp['sfDatabase'],sfOptionstemp['sfSchema'],temp_table_to_edw_src_params['table'])
print("Drop temporary table sql statement (droptempstmt) is:",droptempstmt)
#psf.executesqlstatement(spark, sfOptionstemp,droptempstmt)

logger.info("Temporary table " +temp_table_to_edw_src_params['table'] +" is dropped sucessfully")

spark.catalog.clearCache()

logger.info('Glue job completed successfully')

job.commit()