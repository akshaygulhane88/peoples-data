import traceback
import logging
import gzip

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Dynamodb:
    def __init__(self):
        self.writer = None

    def write_sparkdf_to_dynamodb(self,df,dyn_write_format,dyn_write_mode,options):
        '''
        Write spark dataframe to dynamodb
        :param df: spark dataframe
        :param dyn_write_format: dynamodb write format
        :param dyn_write_mode: dynamodb write mode
        :param options: dynamodb spark options
        '''
        try:
            df.write.format(dyn_write_format)\
            .options(**options) \
            .mode(dyn_write_mode)\
            .save()
            
        except Exception as e:
            logger.error("Dynamodb write fails due to the error" + ".\n{}".format(traceback.format_exc()))
            raise e