import requests
import traceback
import logging

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Api:
    def __init__(self):
        self.Api = None

    # Retrieve data from rest endpoint
    def retrieve_api_data(self, method, url, params, headers, sslcert):
        '''
        :param method: API Get or Post or Put
        :param url: rest endpoint url
        :param headers: API headers
        :param sslcert: ssl certificates for https
        :return Json response as text
        '''
        try:
            response = requests.request(method, url=url, params=params, headers=headers, verify=sslcert)
            print("API response read successfully")
            return response.text

        except Exception as e:
            print("Unable to read API response" + ".\n{}".format(traceback.format_exc()))
            raise e

    # Retrieve full response from rest endpoint


    def retrieve_api_response(self, method, url, params, headers, sslcert):
        '''
        :param method: API Get or Post or Put
        :param url: rest endpoint url
        :param headers: API headers
        :param sslcert: ssl certificates for https
        :return Json response as text
        '''
        try:
            response = requests.request(method, url=url, params=params, headers=headers, verify=sslcert)
            print("API response read successfully")
            return response

        except Exception as e:
            print("Unable to read API response" + ".\n{}".format(traceback.format_exc()))
            raise e
