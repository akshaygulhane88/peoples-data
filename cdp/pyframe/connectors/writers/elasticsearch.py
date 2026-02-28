import traceback
import logging
import gzip
#from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from requests.auth import HTTPBasicAuth
import sys
import boto3
import time
import json

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)    
logger.setLevel(logging.INFO)
#logger.debug("Debugging info with stack", stack_info=True)


class Elasticsearch:
    def __init__(self):
        self.writer = None
        
    def get_elasticsearch_object(self, es_region, es_host, service='es',  es_port=443, use_ssl=True, verify_certs=True):
        '''
        Get elasticsearch instance 
        :param region: region in which es is provisioned
        :param service: 'es' - service name
        :param host: elasticsearch instance hostname
        :param port: elasticsearch instance listening https port number
        :param use_ssl: ssl enabled
        :param verify_certs: verify certificates
        :return es: elasticsearch instance object
        '''
        try:
        
            es_region = es_region
            es_service = service
            es_host = es_host
            es_port = es_port

            session = boto3.Session(region_name=es_region)
            credentials = session.get_credentials()
            credentials = credentials.get_frozen_credentials()
            access_key = credentials.access_key
            secret_key = credentials.secret_key
            token = credentials.token
            
            aws_auth = AWSV4SignerAuth(credentials, es_region, es_service)

            es = OpenSearch(
            hosts = [{'host': str(es_host), 'port': str(es_port)}],
            http_auth=aws_auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            connection_class=RequestsHttpConnection
                                                   
            )
            return es
        
        except Exception as e:
            logger.error("Elasticsearch write fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e


    def write_sparkdf_to_elasticsearch(self,df,es_write_format,es_write_mode,es_index,options):
        '''
        Write spark dataframe to elasticsearch index
        :param df: spark dataframe
        :param es_write_format: elastic search write format
        :param es_write_mode: elastic search write mode
        :param es_index: elastic search index name
        :param options: elastic search spark options
        '''
        try:
            df.write.format(es_write_format)\
            .options(**options) \
            .mode(es_write_mode)\
            .save(es_index)
            
        except Exception as e:
            logger.error("Elasticsearch write fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e

    def get_opensearch_object(self, os_region, os_host, http_auth_username, http_auth_password, service='aoss',  os_port=443, use_ssl=True, verify_certs=True):
        '''
        Get opensearch instance 
        :param region: region in which os is provisioned
        :param service: 'os' - service name
        :param host: opensearch instance hostname
        :param port: opensearch instance listening https port number
        :param use_ssl: ssl enabled
        :param verify_certs: verify certificates
        :return es: opensearch instance object
        '''
        try:
        
            os_region = os_region
            os_service = service
            os_host = os_host
            os_port = os_port

            # session = boto3.Session(region_name=os_region)
            # credentials = session.get_credentials()
            # credentials = credentials.get_frozen_credentials()
            # access_key = credentials.access_key
            # secret_key = credentials.secret_key
            # token = credentials.token

            http_auth = HTTPBasicAuth(http_auth_username,http_auth_password)
            #aws_auth = AWSV4SignerAuth(credentials, os_region, os_service)

            #aws_auth = AWS4Auth(
            #    access_key,
            #    secret_key,
            #    os_region,
            #    os_service,
            #    session_token=token
            #)
            os = OpenSearch(
            hosts = [{'host': str(os_host), 'port': str(os_port)}],
            http_auth=http_auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            connection_class=RequestsHttpConnection
            )
            return os
        
        except Exception as e:
            logger.error("Open search connectivity fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e


    def write_sparkdf_to_opensearch(self,df,os_write_format,os_write_mode,os_index,options):
        '''
        Write spark dataframe to opensearch index
        :param df: spark dataframe
        :param es_write_format: open search write format
        :param es_write_mode: open search write mode
        :param es_index: open search index name
        :param options: open search spark options
        '''
        try:
            df.write.format(os_write_format)\
            .options(**options) \
            .mode(os_write_mode)\
            .save(os_index)
            
        except Exception as e:
            logger.error("Opensearch write fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e
    

    def preprocess(self,es,alias_name,index_name,index_prev_name,config_bucket,schema_key):
        '''
        Switch alias to index_prev, replicate data from index to index_prev_name, and  remove alias from index
        :param es: elasticsearch index object
        :param alias_name: elasticsearch index alias
        :param index_prev_name: elasticsearch index previous
        :return "success"
        ''' 
        try:
            # Switch alias to index_prev and replicate data from index to index_prev_name
            # es.delete_by_query(index=index_prev_name, body={"query": {"match_all": {}}},wait_for_completion=False)
            start_del = time.time()
            if es.indices.exists(index=index_prev_name):
                del_idx_response = es.indices.delete(index = index_prev_name)
                print("response of delete index:",del_idx_response)
            end_del = time.time()
            print("Time taken for deleting an index is:",end_del-start_del)
            index_read_only_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":True}) 
            print("response of read only settings of index:",index_read_only_response)
            
            start_clone = time.time()
            clone_response = es.indices.clone(index_name,index_prev_name) 
            print("response of clone index:",clone_response)
            end_clone = time.time()
            print("Time taken for cloning an index is:",end_clone-start_clone)
            
            # es.reindex(
                # body={"source": {"index": index_name}, "dest": {"index": index_prev_name}},
                # request_timeout=3600,
            # )
            
            # refresh the index to make the changes visible
            es.indices.refresh(index=index_prev_name)
            
            start_alias_switch = time.time()
            es.indices.update_aliases(body={
            "actions": [
                {"remove": {"index": index_name, "alias": alias_name}},
                {"add": {"index": index_prev_name, "alias": alias_name}}
                ]
            })
            end_alias_switch = time.time()
            print("Time taken for alias switching:",end_alias_switch-start_alias_switch)

            start_index_recreate = time.time()
            # Code to read schema file from s3 to drop and re-create index before load
            s3 = boto3.resource("s3")
            os_schema_object = s3.Bucket(config_bucket).Object(schema_key).get()
            os_schema_text = os_schema_object['Body'].read()
            os_schema = os_schema_text.decode()
            schema = json.loads(os_schema)
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name, body=schema)
            end_index_recreate = time.time()
            print(f"Time taken for index - {index_name} recreation:", end_index_recreate - start_index_recreate)
            
            index_write_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":False}) 
            print("response of write settings of index:",index_write_response)
            
            return "success"
            
        except Exception as e:
            logger.error("Elasticsearch reindex fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e

    def refreshincrindexschema(self,es,alias_name,index_name,index_prev_name,config_bucket,schema_key):
        '''
        Switch alias to index_prev, replicate data from index to index_prev_name, remove alias from index, 
        create original index from schema, reindex orig index, switch alias to orig index, and drop prev index
        :param es: elasticsearch index object
        :param alias_name: elasticsearch index alias
        :param index_prev_name: elasticsearch index previous
        :return "success"
        ''' 
        try:
            # Switch alias to index_prev and replicate data from index to index_prev_name
            # es.delete_by_query(index=index_prev_name, body={"query": {"match_all": {}}},wait_for_completion=False)
            
            ######################################################################################
            #1. Drop and clone prev index from orig index
            ######################################################################################
            start_del = time.time()
            if es.indices.exists(index=index_prev_name):
                del_idx_response = es.indices.delete(index = index_prev_name)
                print("response of delete index:",del_idx_response)
            end_del = time.time()
            print("Time taken for deleting an index is:",end_del-start_del)
            index_read_only_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":True}) 
            print("response of read only settings of index:",index_read_only_response)
            
            start_clone = time.time()
            clone_response = es.indices.clone(index_name,index_prev_name) 
            print("response of clone index:",clone_response)
            end_clone = time.time()
            print("Time taken for cloning an index is:",end_clone-start_clone)
            
            
            # refresh the index to make the changes visible
            es.indices.refresh(index=index_prev_name)

            ######################################################################################
            #2. Switch alias to prev index from orig index
            ######################################################################################
            start_alias_switch = time.time()
            es.indices.update_aliases(body={
            "actions": [
                {"remove": {"index": index_name, "alias": alias_name}},
                {"add": {"index": index_prev_name, "alias": alias_name}}
                ]
            })
            end_alias_switch = time.time()
            print("Time taken for alias switching:",end_alias_switch-start_alias_switch)

            start_index_recreate = time.time()
            # Code to read schema file from s3 to drop and re-create index before load
            
            ######################################################################################
            #3. Create original index from schema and reindex orig index from prev index
            ######################################################################################
            s3 = boto3.resource("s3")
            os_schema_object = s3.Bucket(config_bucket).Object(schema_key).get()
            os_schema_text = os_schema_object['Body'].read()
            os_schema = os_schema_text.decode()
            schema = json.loads(os_schema)
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name, body=schema)
            end_index_recreate = time.time()
            start_reindex = time.time()
            es.reindex(
                body={"source": {"index": index_prev_name}, "dest": {"index": index_name}},
                request_timeout=3600,
            )
            
            # refresh the index to make the changes visible
            es.indices.refresh(index=index_name)
            end_reindex = time.time()
            print("Time taken for reindexing is:",end_reindex-start_reindex)
            print(f"Time taken for index - {index_name} recreation:", end_index_recreate - start_index_recreate)
            
            index_write_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":False}) 
            print("response of write settings of index:",index_write_response)

            ######################################################################################
            #4. Switch alias to orig index from prev index
            ######################################################################################
            body = {
            "actions": [
                {"remove": {"index": index_prev_name, "alias": alias_name}},
                {"add": {"index": index_name, "alias": alias_name}}
                ]
            }
            es.indices.update_aliases(body)

            ######################################################################################
            #5. Drop prev index
            ######################################################################################
            if es.indices.exists(index=index_prev_name):
                del_idx_response = es.indices.delete(index=index_prev_name)
                print("response of delete index:", del_idx_response)
            return "success"
            
        except Exception as e:
            logger.error("Elasticsearch reindex fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e
      
    def postprocess(self, es,alias_name,index_name,index_prev_name):
        '''
        Switch alias to index, and drop alias from index previous
        :param es: elasticsearch index object
        :param alias: elasticsearch index alias
        :param index_name: elasticsearch index
        :param index_prev_name: elasticsearch index previous
        :return "success"
        ''' 
        try:
            # Switch alias to index and remove alias from index_prev 
            body = {
            "actions": [
                {"remove": {"index": index_prev_name, "alias": alias_name}},
                {"add": {"index": index_name, "alias": alias_name}}
                ]
            }
            es.indices.update_aliases(body)
            if es.indices.exists(index=index_prev_name):
                del_idx_response = es.indices.delete(index=index_prev_name)
                print("response of delete index:", del_idx_response)
            return "success"
            
        except Exception as e:
            logger.error("Elasticsearch read fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e     
    

    def preprocessold(self,es,alias_name,index_name,index_prev_name):
        '''
        Switch alias to index_prev, replicate data from index to index_prev_name, and  remove alias from index
        :param es: elasticsearch index object
        :param alias_name: elasticsearch index alias
        :param index_prev_name: elasticsearch index previous
        :return "success"
        ''' 
        try:
            # Switch alias to index_prev and replicate data from index to index_prev_name
            # es.delete_by_query(index=index_prev_name, body={"query": {"match_all": {}}},wait_for_completion=False)
            start_del = time.time()
            if es.indices.exists(index=index_prev_name):
                del_idx_response = es.indices.delete(index = index_prev_name)
                print("response of delete index:",del_idx_response)
            end_del = time.time()
            print("Time taken for deleting an index is:",end_del-start_del)
            index_read_only_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":True}) 
            print("response of read only settings of index:",index_read_only_response)
            
            start_clone = time.time()
            clone_response = es.indices.clone(index_name,index_prev_name) 
            print("response of clone index:",clone_response)
            end_clone = time.time()
            print("Time taken for cloning an index is:",end_clone-start_clone)
            
            # es.reindex(
                # body={"source": {"index": index_name}, "dest": {"index": index_prev_name}},
                # request_timeout=3600,
            # )
            
            # refresh the index to make the changes visible
            es.indices.refresh(index=index_prev_name)
            
            start_alias_switch = time.time()
            es.indices.update_aliases(body={
            "actions": [
                {"remove": {"index": index_name, "alias": alias_name}},
                {"add": {"index": index_prev_name, "alias": alias_name}}
                ]
            })
            end_alias_switch = time.time()
            print("Time taken for alias switching:",end_alias_switch-start_alias_switch)
            
            index_write_response = es.indices.put_settings(index=index_name, body={"index.blocks.write":False}) 
            print("response of write settings of index:",index_write_response)
            
            return "sucess"
            
        except Exception as e:
            logger.error("Elasticsearch reindex fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e
        
      
    def postprocessold(self, es,alias_name,index_name,index_prev_name):
        '''
        Switch alias to index, and drop alias from index previous
        :param es: elasticsearch index object
        :param alias: elasticsearch index alias
        :param index_name: elasticsearch index
        :param index_prev_name: elasticsearch index previous
        :return "success"
        ''' 
        try:
            # Switch alias to index and remove alias from index_prev 
            body = {
            "actions": [
                {"remove": {"index": index_prev_name, "alias": alias_name}},
                {"add": {"index": index_name, "alias": alias_name}}
                ]
            }
            es.indices.update_aliases(body)
            return "sucess"
            
        except Exception as e:
            logger.error("Elasticsearch read fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e     
