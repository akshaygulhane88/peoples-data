import os
import json
import logging
import traceback
import logging
from .. import constants
from datetime import datetime
import time
import gzip
import snowflake.connector
import pandas as pd
from pandas import DataFrame
#from opensearchpy import OpenSearch, TransportError

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
current_date = datetime.today().strftime('%Y-%m-%d')
hour = time.strftime("%H")

def getsnowflakeconobj(sfURL,sfAccount,sfUser,sfPassword,sfWarehouse,sfDatabase=None,sfSchema=None,sfRole=None,truncate_table=None,
  usestagingtable=None):
    '''
    Get snowflake connection object
    :param sfURL: snowflake database URL
    :param sfAccount: snowflake database account
    :param sfUser: snowflake database user
    :param sfPassword: snowflake database password
    :param sfWarehouse: snowflake warehouse
    :param sfDatabase: snowflake database
    :param sfSchema: snowflake database schema
    :param sfrole: snowflake database role
    :param truncate_table: truncate_table option (ON/OFF)
    :param usestagingtable: usestagingtable option (ON/OFF)
    :return: snowflake connection object
    '''
    try:
        if sfDatabase is None and sfSchema is None and sfRole is None :
            sfOptions = {
            "sfURL" : sfURL,
            "sfAccount" : sfAccount,
            "sfUser" : sfUser,
            "sfPassword" : sfPassword,
            "sfWarehouse" : sfWarehouse,
            }
        elif truncate_table is None and usestagingtable is None:
            sfOptions = {
            "sfURL" : sfURL,
            "sfAccount" : sfAccount,
            "sfUser" : sfUser,
            "sfPassword" : sfPassword,
            "sfDatabase" : sfDatabase,
            "sfSchema" : sfSchema,
            "sfRole": sfRole,
            "sfWarehouse" : sfWarehouse,
            }
        else:
            sfOptions = {
            "sfURL" : sfURL,
            "sfAccount" : sfAccount,
            "sfUser" : sfUser,
            "sfPassword" : sfPassword,
            "sfDatabase" : sfDatabase,
            "sfSchema" : sfSchema,
            "sfRole": sfRole,
            "sfWarehouse" : sfWarehouse,
            "truncate_table": truncate_table,
            "usestagingtable": usestagingtable,
            }
            
        return sfOptions
    
    except ValueError as e:
        logger.error("Unable to create snowflake connection object" + ".\n{}".format(traceback.format_exc())) 
        raise e
        
    
# Download object from S3 and move to a temp location in any process storage : for eg: Lambda, Glue -> temp directory
def download_s3_file(s3_client,bucket,key,temp_location):
    '''
    :param bucket: bucket name of the file to be downloaded
    :param key: key name of the file to be downloaded
    :temp_location: temporary location of the file
    '''
    try:
        s3_client.meta.client.download_file(bucket, key, temp_location)
        print("S3 file downloaded to temp location")

    except Exception as e:
        logger.error("Unable to download certificate file" + ".\n{}".format(traceback.format_exc()))
        raise e
        
        
# Write data to S3 location
def write_data_to_S3(s3_resource,target_bucket_name,target_key_name,data, target_compression_type):
    '''
    :param target_bucket_name: target s3 bucket for data
    :param target_key_name : target s3 key 
    :param data: output data in text
    '''
    try:
        s3_resource.Object(target_bucket_name, target_key_name).put(Body=gzip.compress(data.encode()))
        print("Data written to s3 successfully")
        
    except Exception as e:
        logger.error("Unable to load Data to S3" + ".\n{}".format(traceback.format_exc()))
        raise e
        

def get_source_s3_path(source_bucket_name, source_key_name):
    '''
    :param source_bucket_name: target s3 bucket for data
    :param source_key_name : target s3 key name
    :return source s3 key path
    '''
    try:
        source_s3_path = constants.S3PREFIX+source_bucket_name+constants.S3PATHDELIM+source_key_name+constants.S3PATHDELIM
        return source_s3_path
    
    except Exception as e:
        logger.error("Unable to get source s3 path" + ".\n{}".format(traceback.format_exc()))
        raise e  
        
def get_target_s3_path(load_type,target_bucket_name,target_key_name):
    '''
    :param load_type: load type : full, incr
    :param target_bucket_name: target s3 bucket for data
    :param target_key_name : target s3 key name
    :return target s3 key path
    '''
    try:
        if load_type==constants.INCR:
            target_s3_path = constants.S3PREFIX+target_bucket_name+constants.S3PATHDELIM+target_key_name+ \
            constants.DATEPARTPREFIX+current_date+constants.S3PATHDELIM+constants.HOURPARTPREFIX+hour+constants.S3PATHDELIM
        else:
            target_s3_path =constants.S3PREFIX+target_bucket_name+constants.S3PATHDELIM+target_key_name+constants.S3PATHDELIM
        print("target_s3_path is :",target_s3_path)
        return target_s3_path
        
    except Exception as e:
        logger.error("Unable to get target s3 path" + ".\n{}".format(traceback.format_exc()))
        raise e   

def get_s3_path(t_bucket,t_key):
    '''
    compiles s3 path with target bucket and key
        from config file
    :param t_bucket: target bucket
    :param t_key: target key
    :return: s3 string path
    '''
    try:
        target_path  = 's3://' + t_bucket + '/' + t_key # test folder removed
        return target_path
    except Exception as e:
        logger.error("Unable to format target path" + ".\n{}".format(traceback.format_exc()))
        raise e


def get_query_format(query_string,load_type, ts=None):
    '''
    formatting custom sql
    :param query_string: query string
    :param audit_column: column for Inc/Full load
    :param timestamp: timestamp to filter audit column
        within where clause
    :return: dynamic sql string
    '''
    try:
        if load_type.lower() == 'full':
            query = query_string
            return query
        elif load_type.lower() == 'incr':
            ''' Temporary hardcoded timestamp
            will either be passed as argument via airflow
            or called stored procedure directly via glue
            '''
            ts = pd.to_datetime(ts)
            timestamp = ts.strftime("%Y-%m-%d %H:%M:%S")
            print('this is formatted email: ',timestamp)
            query = query_string.format(timestamp)
            print('collecting ')
            return query
    except Exception as e:
        logger.error("Unable to format string sql query" + ".\n{}".format(traceback.format_exc()))
        raise e



def getpandasdffromsnowflake(connection:dict,query:str)->DataFrame:
    '''
    :param connection: dictionary containing snowflake connection parameters such as 
     role, acocunt, url, database, user, password, warehouse
    :param query - Query string
    return pandas dataframe
    '''
    try:
        # Enter credentials for the Snowflake connector.
        con = snowflake.connector.connect(
            url=connection['sfurl'],
            user=connection['sfusername'],
            password=connection['sfpassword'],
            account=connection['sfaccount'],
            warehouse=connection['sfwarehouse'],
            role=connection['sfrole'],
            database=connection['sfdatabase']
        )
        cs = con.cursor()
        #cs.execute(query)
        data=cs.execute(query)
        #data = cs.fetch_pandas_all()
        #df = pd.DataFrame(data)
        df = pd.DataFrame.from_records(iter(data), columns=[x[0] for x in data.description])
        return df
    
    except Exception as e:
        logger.error("Unable to connect to snowflake" + ".\n{}".format(traceback.format_exc()))
        raise e
        
    finally:    
        cs.close()
        con.close()
        
        
def execprocsinsnowflake(connection:dict,query:str)-> None:
    '''
    :param connection: dictionary containing snowflake connection parameters such as 
     role, acocunt, url, database, user, password, warehouse
    :param query - Query string
    '''
    try:
        # Enter credentials for the Snowflake connector.
        con = snowflake.connector.connect(
            url=connection['sfurl'],
            user=connection['sfusername'],
            password=connection['sfpassword'],
            account=connection['sfaccount'],
            warehouse=connection['sfwarehouse'],
            role=connection['sfrole'],
            database=connection['sfdatabase']
        )
        cs = con.cursor()
        cs.execute(query)
        print("Snowflake procedure is successfully executed")
    
    except Exception as e:
        logger.error("Unable to connect to snowflake" + ".\n{}".format(traceback.format_exc()))
        raise e
        
    finally:    
        cs.close()
        con.close()

    
def convertnumpydttointeger(datetimestampval):
    '''
    :param dateval: numpy date value
    :return datehour value integer - date & hour value concatenated : 2022021416 (yyyymmddhh format without / or -)
    '''
    try:
        datetmfmt = pd.to_datetime(str(datetimestampval)).replace(tzinfo=None)
        dateval = datetime.strftime(datetmfmt,"%Y-%m-%d")
        hourval = datetime.strftime(datetmfmt,"%H")
        date_hr = (dateval+hourval).replace("-","")
        datehourvalue = int(date_hr)
        return datehourvalue
    
    except Exception as e:
        logger.error("Error in deriving datehour value " + ".\n{}".format(traceback.format_exc()))
        raise e
 
def json_str_replace(jsonobj, variable, value):
        '''
        :param jsonobj: json object
        :param variable: variable name to be replaced
        :param value: actual value to replace with
        :return: modified json object
        '''
        try:
            jsonstr = json.dumps(jsonobj)
            jsonstrrepl = jsonstr.replace(variable, value)
            jsonobj = json.loads(jsonstrrepl)
            return jsonobj
            
        except Exception as e:
            logger.error("Unable to do json object string replace" + ".\n{}".format(traceback.format_exc()))
            raise e
            
# def getopensearchclient(host, port, auth, use_ssl=True,verify_certs = False,ssl_assert_hostname = False,ssl_show_warn = False):
    # '''
    # Get open search client 
    # :param host: String - open search host name
    # :param port: Integer - open search port number
    # :param auth: Tuple - authentication: (user,password)
    # :param use_ssl: Boolean - use ssl or not 
    # :param verify_certs: Boolean - verify https certs
    # :param ssl_assert_hostname: Boolean - SSL assert hostname or not
    # :param ssl_show_warn: SSL show warnings or not
    # :return Opensearch client
    # '''
    # try:
        # client = OpenSearch(
        # hosts = [{'host': host, 'port': port}],
        # http_compress = True, 
        # http_auth = auth,
        # use_ssl = use_ssl,
        # verify_certs = verify_certs,
        # ssl_assert_hostname = ssl_assert_hostname,
        # ssl_show_warn = ssl_show_warn
        # )
        # return client
        
    # except Exception as e:
        # logger.error("Error in creating opensearch client object " + ".\n{}".format(traceback.format_exc()))
        # raise e
 

# def df_doc_generator(df: pd.DataFrame) -> Generator[Dict[str, Any], None, None]:
    # def _deserialize(v: Any) -> Any:
        # if isinstance(v, str):
            # v = v.strip()
            # if v.startswith("{") and v.endswith("}") or v.startswith("[") and v.endswith("]"):
                # try:
                    # v = json.loads(v)
                # except json.decoder.JSONDecodeError:
                    # try:
                        # v = ast.literal_eval(v)  # if properties are enclosed with single quotes
                        # if not isinstance(v, dict):
                            # logger.warning("could not convert string to json: %s", v)
                    # except SyntaxError as e:
                        # logger.warning("could not convert string to json: %s", v)
                        # logger.warning(e)
        # return v

    # df_iter = df.iterrows()
    # for _, document in df_iter:
        # yield {k: _deserialize(v) for k, v in document.items() if notna(v)}
        
        
     
# def index_df(
    # client: OpenSearch, df: pd.DataFrame, index: str, doc_type: Optional[str] = None, **kwargs: Any
# ) -> Dict[str, Any]:
    # """Index all documents from a DataFrame to OpenSearch index.
    # Parameters
    # ----------
    # client : OpenSearch
        # instance of opensearchpy.OpenSearch to use.
    # df : pd.DataFrame
        # Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    # index : str
        # Name of the index.
    # doc_type : str, optional
        # Name of the document type (for Elasticsearch versions 5.x and earlier).
    # **kwargs :
        # KEYWORD arguments forwarded to :func:`~awswrangler.opensearch.index_documents`
        # which is used to execute the operation
    # Returns
    # -------
    # Dict[str, Any]
        # Response payload
        # https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.
    # """
    # return index_documents(client=client, documents=df_doc_generator(df), index=index, doc_type=doc_type, **kwargs)
   


        

    
