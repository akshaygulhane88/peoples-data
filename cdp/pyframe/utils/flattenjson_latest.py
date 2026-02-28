from pyspark.sql.functions import explode, col, lit
import pyspark.sql.types as T
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from . import pysparkfuncs    
import pandas as pd   
import logging
import traceback
import re

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)                     

class JsonFlattenParser():

    def __init__(self):
        self.parse = None
        self.seperator = None
        
        
    def flatten_json(self,y):
        '''
        flattens hieararchical dictionary and returns back the flattened dictionary
        :param y: hierarchical dictionary
        :return out: flattened dictionary
        '''
        try:
            out = {}

            def flatten(x, name=''):
                name = re.sub('[ ,;{}()\\n\\t=]','_',name)
                if type(x) is dict:
                    for a in x:
                        flatten(x[a], name + a + '_')
                elif type(x) is list:
                    i = 0
                    for a in x:
                        flatten(a, name + str(i) + '_')
                        i += 1
                else:
                    if not x:
                        pass
                    out[name[:-1]] = x
                    
            flatten(y)
            return out
            
        except Exception as e:
            logger.error("Unable to flatten json due to: " + ".\n{}".format(traceback.format_exc()))
            raise e
       
        
    def getflatteneddataframe(self,spark,nodes,data,level)->DataFrame:
        '''
        :param spark: spark session client object
        :param data: data as dictionary 
        :return flattened datframe
        ''' 
        try:
            pandasDF=pd.json_normalize(data,nodes,max_level=level)
            pandasDF = pandasDF.dropna(axis='columns',how='all')
            flatteneddf=spark.createDataFrame(pandasDF) 
            cols=[re.sub('[ ,;{}()\\n\\t=]','_',col_name) for col_name in flatteneddf.columns]
            flattenedfmtdf=flatteneddf.toDF(*cols)
            flattenedfmtdf.printSchema()
            flattenedfmtdf.show(100,truncate=False)
            return flattenedfmtdf
        
        except Exception as e:
            logger.error("Unable to create flattened dataframe due to: "+ ".\n{}".format(traceback.format_exc()))
            raise e
        
                
  