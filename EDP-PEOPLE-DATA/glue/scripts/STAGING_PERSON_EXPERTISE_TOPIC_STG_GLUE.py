import sys
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
# '''Reusable
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_bucket', 'config_key', 'environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize logger
logger = glueContext.get_logger()
logger.info("Spark and glue session objects are created successfully")

config_bucket = args['config_bucket']
config_key = args['config_key']
environment = args['environment'].lower()
# 2. Create S3 and snowflake connection objects and get the config object
s3_conn = boto3.client(constants.S3)

try:
    config_obj = configparser.ConfigParser()
    jconfig = config_obj.getjconfigobject(s3_conn, config_bucket, config_key)
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

# Stage: merge_load_view_to_stg params
load_view_to_stg_src_params = stage_params[0]['merge_load_view_to_stg']['sources'][0]
load_view_to_stg_tgt_params = stage_params[0]['merge_load_view_to_stg']['targets'][0]

# Stage: merge_temp_table_to_target params
merger_temp_table_to_target_src_param = stage_params[1]['merger_temp_table_to_target']['sources'][0]
merger_temp_table_to_target_tgt_param = stage_params[1]['merger_temp_table_to_target']['targets'][0]
# -----------------------------------------------------------------------------------------------------------------------------------------------
# Get control params
sfconnection = controlparams['connection']
sfctrldatabase = controlparams['connection']['sfdatabase']
sfctrlschema = controlparams['schema']
sfctrltable = controlparams['name']
dfquery = controlparams['dfquery']
procquery_raw = controlparams['procquery']
snowflake_db_secret_name = env_params['snowflake_db_secret_name']
snowflake_source_name = global_params['snowflake_source_name']

# Snowflake connection object preparation
snowflake_db_secret_name = env_params['snowflake_db_secret_name']
secretobj = secretretriever.Secret()
secret = json.loads(secretobj.get_secret(snowflake_db_secret_name))

sfuser = secret['username']
sfPassword = secret['password']
sfurl = secret['url']
sfaccount = secret['account']
sfwarehouse = secret['warehouse']

sfconnection['sfusername'] = sfuser
sfconnection['sfpassword'] = sfPassword

# Changes made on 06/06/2022 for handling enviroment parameters related changes
sfconnection['sfurl'] = sfurl
sfconnection['sfaccount'] = sfaccount
sfconnection['sfwarehouse'] = sfwarehouse

java_import(spark._jvm, snowflake_source_name)

spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(
    spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

sfgenurl = secret['url']
sfgenaccount = secret['account']
sfgenwarehouse = secret['warehouse']

sfOptionsgen = pf.getsnowflakeconobj(sfgenurl, sfgenaccount, sfuser, sfPassword, sfgenwarehouse)

# Stage 1 : Connection parameters
sfextvurl = secret['url']
sfextvaccount = secret['account']
sfextvdatabase = env_params['database']
sfextvschema = load_view_to_stg_src_params['schema']
sfextvrole = env_params['role']
sfextvwarehouse = secret['warehouse']

sfOptionsextv = pf.getsnowflakeconobj(sfextvurl, sfextvaccount, sfuser, sfPassword, sfextvwarehouse,sfextvdatabase,sfextvschema, sfextvrole)

sfextvurl2 = secret['url']
sfextvaccount2 = secret['account']
sfextvdatabase2 = env_params['database']
sfextvschema2 = load_view_to_stg_tgt_params['schema']
sfextvrole2 = env_params['role']
sfextvwarehouse2 = secret['warehouse']

sfOptionstempstg = pf.getsnowflakeconobj(sfextvurl2, sfextvaccount2, sfuser, sfPassword, sfextvwarehouse2,sfextvdatabase2,sfextvschema2, sfextvrole2)
                                         
# Stage 2 : Connection parameters
sfstg2url = secret['url'] 
sfstg2account = secret['account'] 
sfstg2warehouse = secret['warehouse'] 
sfstg2database = env_params['database']
sfstg2schema = merger_temp_table_to_target_tgt_param['schema']
sfstg2name = merger_temp_table_to_target_tgt_param['name']
sfstg2role = env_params['role']


sfOptionsstg2 = pf.getsnowflakeconobj(sfstg2url,sfstg2account,sfuser,sfPassword,sfstg2warehouse,sfstg2database,sfstg2schema,sfstg2role)

sftempurl = secret['url']
sftempaccount = secret['account']
sftempwarehouse = secret['warehouse']
sftempdatabase = env_params['database']
sftempschema = merger_temp_table_to_target_src_param['schema']
sftemprole = env_params['role']


sfOptionstemp = pf.getsnowflakeconobj(sftempurl,sftempaccount,sfuser,sfPassword,sftempwarehouse,sftempdatabase,sftempschema,sftemprole)                                         

# -----------------------------------------------------------------------------------------------------------------------------------------------
# Get pandas dataframe of control table
# Get control params

controlpdf = pf.getpandasdffromsnowflake(sfconnection, dfquery)
# print("controlpdf is:", controlpdf.head())

start_timestamp = controlpdf['START_TIMESTAMP'].values[0]
end_timestamp = controlpdf['END_TIMESTAMP'].values[0]

start_timestamp_str = np.datetime_as_string(controlpdf['START_TIMESTAMP'].values[0])
end_timestamp_str = np.datetime_as_string(controlpdf['END_TIMESTAMP'].values[0])

print("start timestamp is :", start_timestamp)
print("End timestamp is :", end_timestamp)

# ----------------------------------------------------------------------------------------------------------------------------------------------

# Update control table by calling stored procedure
# Proc start timestamp query
procstarttsquery = procquery_raw.format("'END_TIMESTAMP'", "'CURRENT_TIMESTAMP()'")
print("proc start ts query is", procstarttsquery)
pf.execprocsinsnowflake(sfconnection, procstarttsquery)
# -----------------------------------------------------------------------------------------------------------------------------------------------
# Business logic Implementation:
# Stage 1 - Create spark temporary views of staging table, dimension & lookup tables, cache the tables that are re-used more than once

# -----------------------------------------------------------------------------------------------------------------------------------------------
# Parse and get create_spark_temp_vws_from_sources params
# Source params
tmp_vw_query_map = load_view_to_stg_src_params['tmp_vw_query_map']
tmp_vw_table_map = load_view_to_stg_src_params['tmp_vw_table_map']

if len(tmp_vw_query_map) > 0:
    for map in tmp_vw_query_map:
        for key, value in map.items():
            sparktmpvwnm = key
            doCache = value[0]
            query = value[1]
            try:
                print("Temporary view is ", sparktmpvwnm)
                print("Query used to create temporary view is ", query)
                psf.createsparktempviewfromquery(spark, snowflake_source_name, sfOptionstempstg, query, sparktmpvwnm,
                                                 doCache)
                print("spark temporary view is created {0}".format(sparktmpvwnm))
                # spark.sql("""select * from {0} """.format(sparktmpvwnm)).show()

            except Exception as e:
                logger.error(
                    "Unable to create spark temporary view: " + sparktmpvwnm + ".\n{}".format(traceback.format_exc()))
                raise e

if len(tmp_vw_table_map) > 0:
    for map in tmp_vw_table_map:
        for key, value in map.items():
            sparktmpvwnm = key
            doCache = value[0]
            db_schema_table = value[1].split(".")
            dbname = db_schema_table[0]
            schema = db_schema_table[1]
            table = db_schema_table[2]
            sfOptionstempstg['sfSchema'] = schema
            sfOptionstempstg['sfDatabase'] = dbname
            # print(db_schema_table)
            # print(sfOptionsstg1)

            try:
                print("Temporary view is ", sparktmpvwnm)
                print("Table used to create temporary view is ", table)
                psf.createsparktempviewfromtable(spark, snowflake_source_name, sfOptionstempstg, table, sparktmpvwnm,
                                                 doCache)
                print("spark temporary view is created {0}".format(sparktmpvwnm))
                # spark.sql("""select * from {0} """.format(sparktmpvwnm)).show()

            except Exception as e:
                logger.error(
                    "Unable to create spark temporary view: " + sparktmpvwnm + ".\n{}".format(traceback.format_exc()))
                raise e

logger.info("Spark temporary views are created")

# 2. Apply business transformation logic and Join the temporary views to create the PERSON_EXPERTISE_TOPIC staging table

paloadtmpvw = load_view_to_stg_tgt_params['name']

load_query = """SELECT DISTINCT FMNO,
				TOPIC_ID,
				TOPIC.NAME AS TOPIC_NAME,
				TOPIC.DESCRIPTION AS TOPIC_DESCRIPTION,
				LINKAGE.TAXONOMY_URL AS TOPIC_TAXONOMY_URL_ID,
				LINKAGE.PRACTICE_URL AS TOPIC_PRACTICE_URL_ID,
				CASE WHEN TAXONOMY.PRACTICE_TAXONOMY='Y' THEN 'FIRM' 
						WHEN TAXONOMY.PRACTICE_TAXONOMY='N' THEN 'SEMANTIC' 
					ELSE '' END TOPIC_TAXNOMY_TYPE,
				TAXONOMY.PREF_LABEL AS TAXONOMY_TAG_VALUE,
                PERSON_TOPIC.PRACTICE_RECOMMENDED AS PRACTICE_RECOMMENDED, 
                PERSON_TOPIC.FIRST_ALERT AS FIRST_ALERT,
                CAST(null AS string) as END_DATE,                                                   -- 45623745924528134766
                CAST(SHA1(NVL(CAST(FMNO AS STRING),'') ||'|~|'||
                                NVL(CAST(TOPIC_ID AS STRING),'') ||'|~|'||
                                NVL(CAST(TOPIC.NAME AS STRING),'') ||'|~|'||
                                NVL(CAST(TOPIC.DESCRIPTION AS STRING),'') ||'|~|'||
                                NVL(CAST(LINKAGE.TAXONOMY_URL AS STRING),'') ||'|~|'||
                                NVL(CAST(LINKAGE.PRACTICE_URL AS STRING),'') ||'|~|'||
                                NVL(CAST(TAXONOMY.PRACTICE_TAXONOMY AS STRING),'') ||'|~|'||
                                NVL(CAST(TAXONOMY.PREF_LABEL AS STRING),'') ||'|~|'||
                                NVL(CAST(PERSON_TOPIC.PRACTICE_RECOMMENDED AS STRING),'') ||'|~|'||
                                NVL(CAST(PERSON_TOPIC.FIRST_ALERT AS STRING),'')) AS STRING) AS CHANGE_HASH
				FROM PERSON_TOPIC as PERSON_TOPIC,
				PERSON as PERSON,
				TOPIC as TOPIC,
				LINKAGE as LINKAGE,
				TAXONOMY as TAXONOMY
				WHERE PERSON_TOPIC.PRACTICE_PERSON_ID=PERSON.ID
				AND PERSON_TOPIC.TOPIC_ID=TOPIC.ID
				AND PERSON.PRACTICE_ID=LINKAGE.PRACTICE_IDENTIFIER
				AND LINKAGE.TAXONOMY_URL=TAXONOMY.URI
				 AND LINKAGE.delete_flag = 'N'
            """.format(paloadtmpvw)

spksqlopts = job_params['spark_options']

print("query (person_expertise_topic_temp_load_sdf) is:", load_query) 

person_expertise_topic_temp_load_sdf = psf.getdffromsparksqlqry(spark, spksqlopts, load_query)

logger.info("Spark temporary dataframe person_expertise_topic_temp_load_sdf is created successfully")

print("sfDatabase is :",sfOptionstemp['sfDatabase'])
print("sfSchema is :",sfOptionstemp['sfSchema'])
print("table is :",merger_temp_table_to_target_src_param['table'])

# Create the temporary table on top of transformed data

temptblddlstmt = """
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
    END_DATE TIMESTAMPNTZ,
	UPDATE_DATE	TIMESTAMPNTZ,
	CHANGE_HASH VARCHAR);
    """.format(merger_temp_table_to_target_src_param['table'])

print("Temp table DDL (temptblddlstmt) is:", temptblddlstmt)

psf.executesqlstatement(spark, sfOptionstemp, temptblddlstmt)

logger.info("Temporary table " + merger_temp_table_to_target_src_param['table'] + " is created successfully in TEMP schema")

# Write the transformed data into temporary staging table

schema = sfOptionstemp['sfSchema']
table2 = load_view_to_stg_tgt_params['name']
write_mode = load_view_to_stg_tgt_params['write_mode']
psf.loaddftotable(person_expertise_topic_temp_load_sdf, snowflake_source_name, sfOptionstemp, schema, table2, write_mode)
# 3. Merge into staging tables from external load views

'''
This stage is used to merge data from the load staging views to the person_expertise_topic staging table
PETS - person_expertise_topic Stg
PETT = person_expertise_topic Temp
'''
#Delete query to Soft-Delete PERSON_EXPERTISE_TOPIC PETS
delstmt = """
update {0}.{1}.PERSON_EXPERTISE_TOPIC_STG TGT
            SET 	END_DATE = CURRENT_TIMESTAMP(),
					UPDATE_DATE = CURRENT_TIMESTAMP(),
					CHANGE_HASH = SHA1('DELETED')
    WHERE NOT EXISTS (SELECT 1 from {2}.TEMP.PERSON_EXPERTISE_TOPIC_STG_TMP SRC
                                where TGT.FMNO = SRC.FMNO
                                 AND TGT.TOPIC_ID = SRC.TOPIC_ID
                                 AND TGT.TOPIC_NAME = SRC.TOPIC_NAME
                                 AND TGT.TOPIC_TAXONOMY_URL_ID = SRC.TOPIC_TAXONOMY_URL_ID
                                 AND TGT.END_DATE is Null
)""".format(sfOptionsstg2['sfDatabase'],sfOptionsstg2['sfSchema'],sfOptionstemp['sfDatabase'])

print("Soft-delete statement (delstmt) is:", delstmt)

pcrrstgstmtstatus = psf.executesqlstatement(spark, sfOptionsstg2, delstmt)

logger.info("Soft-delete statement (delstmt) is executed successfully")

#Merge query to Load PERSON_EXPERTISE_TOPIC PETS
paloadstmt = """
MERGE INTO {0}.{1}.PERSON_EXPERTISE_TOPIC_STG PETS
    USING {2}.TEMP.PERSON_EXPERTISE_TOPIC_STG_TMP PETT
    ON PETS.FMNO = PETT.FMNO
    AND PETS.TOPIC_ID = PETT.TOPIC_ID
    AND PETS.TOPIC_NAME = PETT.TOPIC_NAME
    AND PETS.TOPIC_TAXONOMY_URL_ID = PETT.TOPIC_TAXONOMY_URL_ID
	WHEN MATCHED AND PETS.CHANGE_HASH <> PETT.CHANGE_HASH
    THEN
UPDATE SET
    PETS.FMNO = PETT.FMNO,
    PETS.TOPIC_ID = PETT.TOPIC_ID,
	PETS.TOPIC_NAME = PETT.TOPIC_NAME,
	PETS.TOPIC_DESCRIPTION = PETT.TOPIC_DESCRIPTION,
	PETS.TOPIC_TAXONOMY_URL_ID = PETT.TOPIC_TAXONOMY_URL_ID,
	PETS.TAXONOMY_TAG_VALUE = PETT.TAXONOMY_TAG_VALUE,
	PETS.TOPIC_TAXNOMY_TYPE = PETT.TOPIC_TAXNOMY_TYPE, 
	PETS.TOPIC_PRACTICE_URL_ID = PETT.TOPIC_PRACTICE_URL_ID,
    PETS.PRACTICE_RECOMMENDED = PETT.PRACTICE_RECOMMENDED,
    PETS.FIRST_ALERT = PETT.FIRST_ALERT,
    PETS.END_DATE = PETT.END_DATE,
	PETS.UPDATE_DATE = CURRENT_TIMESTAMP(),
    PETS.CHANGE_HASH = PETT.CHANGE_HASH
    WHEN NOT MATCHED
    THEN
INSERT
(
    FMNO ,
	TOPIC_ID ,
	TOPIC_NAME ,
	TOPIC_DESCRIPTION ,
	TOPIC_TAXONOMY_URL_ID ,
	TAXONOMY_TAG_VALUE ,
	TOPIC_TAXNOMY_TYPE , 
	TOPIC_PRACTICE_URL_ID ,
    PRACTICE_RECOMMENDED ,
    FIRST_ALERT ,
    START_DATE,
    END_DATE ,
    CREATION_DATE,
	UPDATE_DATE	,
	CHANGE_HASH
)
VALUES
(
    PETT.FMNO ,
	PETT.TOPIC_ID ,
	PETT.TOPIC_NAME ,
	PETT.TOPIC_DESCRIPTION ,
	PETT.TOPIC_TAXONOMY_URL_ID ,
	PETT.TAXONOMY_TAG_VALUE ,
	PETT.TOPIC_TAXNOMY_TYPE , 
	PETT.TOPIC_PRACTICE_URL_ID ,
    PETT.PRACTICE_RECOMMENDED ,
    PETT.FIRST_ALERT ,
    CURRENT_TIMESTAMP(),
    PETT.END_DATE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    PETT.CHANGE_HASH
)""".format(sfOptionsstg2['sfDatabase'],sfOptionsstg2['sfSchema'],sfOptionstemp['sfDatabase'])

print("merge statement (paloadstmt) is:", paloadstmt)

pcrrstgstmtstatus = psf.executesqlstatement(spark, sfOptionsstg2, paloadstmt)

logger.info("merge statement (paloadstmt) is executed successfully")

# -----------------------------------------------------------------------------------------------------------------------------------------------
# 10. Update control table by calling stored procedure
# Proc end timestamp query
procendtsquery = procquery_raw.format("'START_TIMESTAMP'", "'END_TIMESTAMP'")
print("proc end ts query is", procendtsquery)
pf.execprocsinsnowflake(sfconnection, procendtsquery)

# -----------------------------------------------------------------------------------------------------------------------------------------------
#Post-processing: Cleanup operation
droptempstmt = """DROP TABLE IF EXISTS {0}.{1}.{2};""".format(sfOptionstemp['sfDatabase'],sfOptionstemp['sfSchema'],merger_temp_table_to_target_src_param['table'])
print("Drop temporary table sql statement (droptempstmt) is:", droptempstmt)
psf.executesqlstatement(spark, sfOptionstemp, droptempstmt)
logger.info("Temporary table " + merger_temp_table_to_target_src_param['table'] + " is dropped sucessfully")

logger.info('Glue job completed successfully')
job.commit()