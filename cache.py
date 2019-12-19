import codecs
import glob
import os
import time
import requests
import settings
import datetime
import logging
from retrying import retry

def seconds_to_milliseconds(seconds):
    return seconds*1000

@retry(wait_exponential_multiplier=seconds_to_milliseconds(1), wait_exponential_max=seconds_to_milliseconds(settings.WAIT_BEFORE_TIMEOUT))
def stubborn_request(url):
    try:
        output = requests.get(url,
                              timeout=120,
                              headers=settings.HEADERS)
        logging.info("Request to " + url + " succeeded.")
        return output
    except Exception:
        logging.warning("Requests threw an exception. Waiting before trying again...")
        raise IOError


class DataCache:
    last_repec_request_time = datetime.datetime.now()
    last_citec_request_time = datetime.datetime.now()

    @classmethod
    def reset_citec_wait_time(cls):
        cls.last_citec_request_time = datetime.datetime.now()

    @classmethod
    def reset_repec_wait_time(cls):
        cls.last_repec_request_time = datetime.datetime.now()

    @classmethod
    def get_repec_wait_time(cls):
        current = datetime.datetime.now()
        diff = current - cls.last_repec_request_time
        return max(settings.REPEC_WAIT_BETWEEN_REQUESTS - diff.seconds, 0)

    @classmethod
    def get_citec_wait_time(cls):
        current = datetime.datetime.now()
        diff = current - cls.last_citec_request_time
        return max(settings.CITEC_WAIT_BETWEEN_REQUESTS - diff.seconds, 0)

    def __init__(self, cache_location=settings.CACHE_LOCATION):
        # append final '/' if not included in path
        if not cache_location.endswith("/"):
            cache_location = cache_location + "/"

        self.cache_location = cache_location
        self.repec_list = glob.glob(cache_location + "repec" + "/*/*")
        self.citec_list = glob.glob(cache_location + "citec" + "/*/*")

    def _build_handle_from_filename(self, filename):
        return filename.replace("_", "/").split(".")[0]

    def request_repec(self, handle):
        def build_file_path(filename):
            return build_file_directory(filename) + filename

        def build_file_directory(filename):
            filename_components = filename.split(":")
            return self.cache_location + "repec/" + filename_components[1] + "/"

        corresp_file = handle.replace("/", "_") + ".html"
        corresp_dir = build_file_directory(corresp_file)

        if build_file_path(corresp_file) in self.repec_list:
            file = codecs.open(build_file_path(corresp_file), 'r')
            return file.read()
        else:
            time.sleep(DataCache.get_repec_wait_time())
            request = stubborn_request("https://ideas.repec.org/cgi-bin/h.cgi?h=" + handle)
            DataCache.reset_repec_wait_time()
            if not os.path.exists(corresp_dir):
                os.makedirs(corresp_dir)
            file = open(build_file_path(corresp_file), 'w')
            file.write(request.text)
            file.close()
            self.repec_list.append(corresp_file)
            return request.text

    def request_citec(self, handle):
        def build_file_path(filename):
            return build_file_directory(filename) + filename

        def build_file_directory(filename):
            filename_components = filename.split(":")
            return self.cache_location + "citec/" + filename_components[1] + "/"

        corresp_file = handle.replace("/", "_") + ".xml"
        corresp_dir = build_file_directory(corresp_file)

        if build_file_path(corresp_file) in self.citec_list:
            file = codecs.open(build_file_path(corresp_file), 'r')
            return file.read()
        else:
            time.sleep(DataCache.get_citec_wait_time())
            request = stubborn_request("http://citec.repec.org/api/citedby/" + handle + "/" + settings.CITEC_USERNAME)
            DataCache.reset_citec_wait_time()
            if not os.path.exists(corresp_dir):
                os.makedirs(corresp_dir)
            file = open(build_file_path(corresp_file), 'w')
            file.write(request.text)
            file.close()
            self.citec_list.append(corresp_file)
            return request.text