import traceback
import logging
import gzip

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class solr:
    def __init__(self):
        self.reader = None

    def read_sparkdf_from_solr(self,spark,solr_read_format,options):
        '''
        Read spark dataframe from elasticsearch index
        :param spark: spark session
        :param solr_read_format: solr read format
        :return df: dataframe
        '''
        try:
            df = spark.read.format(solr_read_format) \
            .options(**options) \
            .load() 
            return df
            
            
        except Exception as e:
            logger.error("Solr read fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e
