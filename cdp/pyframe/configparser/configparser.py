import boto3
from botocore.exceptions import ClientError
import logging
from .. import constants
import traceback
import json
from cdp.pyframe.utils.pythonfuncs import json_str_replace

logger = logging.getLogger(__name__)


class ConfigParser():
    def __init__(self):
        print("Initiating reading config file..")
        self.config_object = None

    @classmethod
    def gets3object(cls, s3c, bucket, key):
        '''
        Get object from s3
        :param s3c: s3 client object
        :param bucket: s3 bucket
        :param key: s3 key
        :return: object from s3
        '''
        try:
            return s3c.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

        except Exception as e:
            logger.error("Unable to return s3 object" + ".\n{}".format(traceback.format_exc()))
            raise e


    @classmethod
    def getrawconfigobject(cls, s3c, bucket, key):
        '''
        :param s3c: s3 client object
        :param bucket: s3 bucket
        :param key: s3 key
        :return: config object
        '''
        try:
            config_object = cls.gets3object(s3c, bucket, key)
            return config_object


        except Exception as e:
            logger.error("Unable to read s3 object" + ".\n{}".format(traceback.format_exc()))
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


    def getjconfigobject(self, s3c, bucket, key):
        '''
        Get config dictionary object
        :param s3c: S3 client object
        :param bucket: S3 bucket
        :param key: S3 key
        :return: dictionary config object
        '''
        try:
            jconfig = json.loads(ConfigParser.getrawconfigobject(s3c, bucket, key))
            return jconfig

        except Exception as e:
            logger.error("Unable to read s3 object" + ".\n{}".format(traceback.format_exc()))
            raise e
            
            
    def getsectionparameters(self,jconfig,configsecion,dynamicvars=None):
        '''
        Get sectional parameter object from config
        :param jconfig: dictionary config object
        :return: config section parameter object
        '''            
        try:
            if dynamicvars is None:
                sectionparams = jconfig.get(configsecion)
            else:
                sectionparamsraw = jconfig.get(configsecion)
                for key,value in dynamicvars.items():
                    sectionparamsraw = json_str_replace(sectionparamsraw,key,value)
                sectionparams = sectionparamsraw
            return sectionparams
                

        except ValueError as e:
            logger.error("section parameter does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e
    


    def getglobalparameters(self, jconfig):
        '''
        Get global parameter object from config
        :param jconfig: dictionary config object
        :return: global parameter object
        '''
        try:
            return jconfig.get(constants.GLOBAL)

        except ValueError as e:
            logger.error("Global parameter section does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e


    def getenvironmentparameters(self, jconfig):
        '''
        Get global parameter object from config
        :param jconfig: dictionary config object
        :return: global parameter object
        '''
        try:
            return jconfig.get(constants.ENVIRONMENT)

        except ValueError as e:
            logger.error("Environment parameter section does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e


    def getjobparameters(self, jconfig):
        '''
        Get job parameter object from config
        :param jconfig: dictionary config object
        :return: job parameter object
        '''
        try:
            return jconfig.get(constants.JOB)

        except ValueError as e:
            logger.error("Job parameter section does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e


    def getstagesparameters(self, jconfig, dynamicvars=None):
        '''
        Get job parameter object from config
        :param jconfig: dictionary config object
        :param dynamucvars: dictionary of dynamic variables with values
        :return: job parameter object
        '''
        try:
            if dynamicvars is None:
                stagesparams = jconfig.get(constants.STAGES)
            else:
                stagesparamsraw = jconfig.get(constants.STAGES)
                for key,value in dynamicvars.items():
                    stagesparamsraw = json_str_replace(stagesparamsraw,key,value)
                stagesparams = stagesparamsraw
            return stagesparams
                

        except ValueError as e:
            logger.error("Stage parameter section does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e


    def getcontrolparameters(self, jconfig,dynamicvars=None):
        '''
        Get control parameter object from config
        :param jconfig: dictionary config object
        :param dynamucvars: dictionary of dynamic variables with values
        :return: job parameter object
        '''
        try:
            if dynamicvars is None:
                controlparams = jconfig.get(constants.CONTROL)
            else:
                controlparamsraw = jconfig.get(constants.CONTROL)
                for key,value in dynamicvars.items():
                    controlparamsraw = json_str_replace(controlparamsraw,key,value)
                controlparams = controlparamsraw
            return controlparams

        except ValueError as e:
            logger.error("Global parameter section does not exist" + ".\n{}".format(traceback.format_exc()))
            raise e
    



