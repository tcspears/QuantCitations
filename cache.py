import codecs
import glob
import os
import time
import requests
import settings


class DataCache:
    def __init__(self, cache_location, wait_time=2):
        self.cache_location = cache_location
        self.wait_time = wait_time
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
            request = requests.get("https://ideas.repec.org/cgi-bin/h.cgi?h=" + handle)
            if not os.path.exists(corresp_dir):
                os.makedirs(corresp_dir)
            file = open(build_file_path(corresp_file), 'w')
            file.write(request.text)
            file.close()
            self.repec_list.append(corresp_file)
            time.sleep(self.wait_time)
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
            request = requests.get("http://citec.repec.org/api/citedby/" + handle + "/" + settings.CITEC_USERNAME)
            if not os.path.exists(corresp_dir):
                os.makedirs(corresp_dir)
            file = open(build_file_path(corresp_file), 'w')
            file.write(request.text)
            file.close()
            self.citec_list.append(corresp_file)
            time.sleep(self.wait_time)
            return request.text