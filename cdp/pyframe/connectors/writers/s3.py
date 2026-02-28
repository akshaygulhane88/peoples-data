import traceback
import logging
import gzip

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3:
    def __init__(self):
        self.writer = None

    def write_dataframe_to_S3(self, sdf, fileformat, path, mode, options, partition_columns=None):
        '''
        :param sdf: spark dataframe
        :param fileformat: file format - for eg:json
        :param options: spark read options as a dictionary
        :param path: S3 path
        :param mode: write mode
        '''
        try:
            if partition_columns is None:
                sdf.write.format(fileformat).options(**options).save(path, mode=mode)
                
            else:
                sdf.partitionBy(partition_columns).write.format(fileformat).options(**options).save(path, mode=mode)

        except Exception as e:
            logger.error("Unable to create spark dataframe" + ".\n{}".format(traceback.format_exc()))
            raise e
