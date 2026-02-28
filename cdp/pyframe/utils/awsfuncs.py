import os
import json
import logging
import traceback
from .. import constants
from datetime import datetime
import time
from botocore.exceptions import ClientError
import boto3
from pprint import pprint
import pandas as pd
from datetime import datetime, timedelta


MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
current_date = datetime.today().strftime('%Y-%m-%d')
hour = time.strftime("%H")

def get_matching_s3_keys(s3_conn,bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    key = None
    while True:
        resp = s3_conn.list_objects_v2(**kwargs)
        if resp.get('Contents') is not None:
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        
        else:
            yield None
            break
                
                

            
def getincrraws3paths(source_bucket_name,datehourvalue,raws3keyslist):
    """
    Get the final s3keys list which are to be processed in the formatted layer
    :param source_bucket_name: source bucket name
    :param raws3keyslist: list of s3 keys
    :param datehourvalue: datehour value in integer (YYYYMMDDHH)
    :return finals3keyslist
    """
    try:
        rawkeyslist=[]
        for key in raws3keyslist:
            rawkeysdict = {}
            file = key.split("/")[-1]
            if (constants.DATEPARTPREFIX in key) and (constants.HOURPARTPREFIX in key) and (file!=''):
                hour_value = key.split(constants.S3PATHDELIM)[-2].strip(constants.HOURPARTPREFIX)
                date_value = key.split(constants.S3PATHDELIM)[-3].strip(constants.DATEPARTPREFIX)
                date_hour_str = (date_value+hour_value).replace(constants.HYPHEN,constants.BLANK)
                date_hour_dt = datetime.strptime(date_hour_str, '%Y%m%d%H')
                print("date_hour_dt is ",date_hour_dt)
                date_hour = date_hour_dt + timedelta(hours=3)
                date_hour_value_str = date_hour.strftime('%Y%m%d%H')
                date_hour_value = int(date_hour_value_str)
                rawkeysdict[date_hour_value]=key
            if bool(rawkeysdict)==True:
                rawkeyslist.append(rawkeysdict)

        print("raw s3 keys list is :",rawkeyslist)

        source_s3paths_list=[]
        for keydict in rawkeyslist:
            for key, value in keydict.items():
                if key > datehourvalue:
                    s3path = constants.S3PREFIX+source_bucket_name+constants.S3PATHDELIM+value
                    source_s3paths_list.append(s3path)
            
        print("source s3 paths list is :",source_s3paths_list) 
        return source_s3paths_list
                
    except Exception as e:
        logger.error("Unable to get final s3 keys list that are needed to be processed" + ".\n{}".format(traceback.format_exc()))
        raise e
        
        
def rundynamopartiql(dyn_resource, statement, params):
    """
    Runs a PartiQL statement. A Boto3 resource is used even though
    `execute_statement` is called on the underlying `client` object because the
    resource transforms input and output from plain old Python objects (POPOs) to
    the DynamoDB format. If you create the client directly, you must do these
    transforms yourself.
    :param dyn_resource: Dynamodb resource object
    :param statement: The PartiQL statement.
    :param params: The list of PartiQL parameters. These are applied to the
                   statement in the order they are listed.
    :return: items returned from the statement which is a list of dictionaries.
    """
    try:
        output = dyn_resource.meta.client.execute_statement(
            Statement=statement, Parameters=params)
            
    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(
                "Couldn't execute PartiQL '%s' because the table does not exist.",
                statement)
        else:
            logger.error(
                "Couldn't execute PartiQL '%s'. Here's why: %s: %s", statement,
                err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return output


def rundynamopartiqlbatch(dyn_resource, statements, param_list):
    """
    Runs a PartiQL statement. A Boto3 resource is used even though
    `execute_statement` is called on the underlying `client` object because the
    resource transforms input and output from plain old Python objects (POPOs) to
    the DynamoDB format. If you create the client directly, you must do these
    transforms yourself.
    :param dyn_resource: Dynamodb resource object
    :param statements: The batch of PartiQL statements.
    :param param_list: The batch of PartiQL parameters that are associated with
                       each statement. This list must be in the same order as the
                       statements.
    :return: The responses returned from running the statements, if any.
    """
    try:
        output = dyn_resource.meta.client.batch_execute_statement(
            Statements=[{
                'Statement': statement, 'Parameters': params
            } for statement, params in zip(statements, param_list)])
    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(
                "Couldn't execute batch of PartiQL statements because the table "
                "does not exist.")
        else:
            logger.error(
                "Couldn't execute batch of PartiQL statements. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return output
