import traceback
import logging
import gzip

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Elasticsearch:
    def __init__(self):
        self.reader = None

    def read_sparkdf_from_elasticsearch(self,spark,es_read_format,es_index,options,limit=None,select_columns=None):
        '''
        Read spark dataframe from elasticsearch index
        :param df: spark dataframe
        :param es_read_format: elastic search read format
        :param es_index: elastic search index name
        :param options: elastic search spark options
        :param limit: limit number of rows
        :param select_columns: columns to select
        :return df: dataframe
        '''
        try:
            if limit is None and select_columns is None:
                df = spark.read.format(es_read_format) \
                .options(**options) \
                .load(es_index) 
            elif limit is not None and select_columns is None:
                df = spark.read.format(es_read_format) \
                .options(**options) \
                .load(es_index) \
                .limit(limit)
            elif limit is None and select_columns is not None:
                df = spark.read.format(es_read_format) \
                .options(**options) \
                .load(es_index) \
                .select(select_columns)
            else:
                df = spark.read.format(es_read_format)\
                .options(**options) \
                .load(es_index) \
                .select(select_columns) \
                .limit(limit)
            return df
            
            
        except Exception as e:
            logger.error("Elasticsearch read fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e
