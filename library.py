import codecs
import glob
import json
import queue
import time
from collections import namedtuple
import dateparser
import requests
from bs4 import BeautifulSoup
import os

from sqlalchemy_utils import Ltree

import db
import settings


class NoDataException(Exception):
    pass


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


# Defines named tuple Article for storing article data
Article = namedtuple('Article', 'handle title pub_year pub_venue authors abstract url keywords')


def get_repec_data(cache, repec_handle):
    request = cache.request_repec(repec_handle)
    html_output = BeautifulSoup(request, 'html.parser')

    # Extract article description JSON from HTML
    article_json = json.loads(html_output.find("script", {"type": "application/ld+json"}).text)
    json_text = article_json["@graph"]

    # Search through JSON object and find the relevant information
    article = db.Article()
    article.handle = repec_handle

    for key in json_text:
        for inner_key in key:
            if inner_key == '@id' and key[inner_key] == "#periodical":
                article.pub_venue = key['name'].strip()
            elif inner_key == '@id' and key[inner_key] == '#number':
                article.pub_year = dateparser.parse(key['datePublished']).year
            elif inner_key == '@id' and key[inner_key] == '#article':
                article.title = key['name'].strip()
                article.authors = [author.strip() for author in key['author'].split('&')]
                article.url = key['url'].strip()
                article.abstract = key['description'].strip()
                article.keywords = [keyword.strip() for keyword in key['keywords'].split(';')]

    return article


# Given a repec_handle, returns a
def get_citec_cites(cache, repec_handle):
    request = cache.request_citec(repec_handle)
    xml_output = BeautifulSoup(request, 'html.parser')

    # Check to see if IP is being blocked; if so, raise exception
    error_string = xml_output.find_all("errorstring")
    if len(error_string) > 0:
        if str(error_string[0]) == '<errorstring>Requested document not found</errorstring>':
            raise NoDataException("Requested document not found for " + repec_handle)
        else:
            raise Exception('CiTeC blocking our IP. Failed at: ' + repec_handle)

    # Get corresponding citec URLs
    cite_url_string = xml_output.find_all("text")
    url_text = [element.get('ref') for element in cite_url_string]

    # Extract RePEC handle from URL
    repec_handles = [url.split('.org/')[1] for url in url_text]

    return repec_handles


# Spiders RePEC and Citec from a list of seed papers
def spidering_algorithm(db_session,
                        cache,
                        seed_handles,
                        max_links=100):

    def build_ltree(citation_list):
        return ".".join(list(map(str, citation_list)))

    # Define a namedtuple that will keep track of elements in queue. This consists of a RePec handle
    # and a list defining a citation path to that handle. The seed handles will just use an empty list
    # for the citation path.
    article_info = namedtuple("articleinfo", "handle citation_chain")

    # Initialize Queue object to store RePEC handles; fill it with seed handles.
    repec_queue = queue.Queue()
    visited_handles = list()

    for handle in seed_handles:
        # Because these articles are at the 'root' of the citation chain, the chain list is empty
        repec_queue.put(article_info(handle, []))

    # Initiate counter for article entries and link count
    article_counter = 1
    link_count = 0

    # Spider through the queue
    while not repec_queue.empty():

        current = repec_queue.get()

        if current.handle not in visited_handles:

            try:
                # Download RePEC data and add current counter value as article ID
                print("Getting RePEC data for " + current.handle)
                article = get_repec_data(cache, current.handle)
                article.id = article_counter

                if link_count < max_links:
                    # If we are below max_links, then get citec cites and add them to the queue
                    print("Getting cites for " + current.handle)
                    cites = get_citec_cites(cache, current.handle)
                    for handle in cites:
                        # Second part takes current citation chain and appends current link counter onto it:
                        # e.g., [1,2] -> [1,2,3].
                        to_put = article_info(handle, current.citation_chain + [article_counter])
                        repec_queue.put(to_put)
                        print("link count : " + str(link_count))
                        link_count += 1
                        if link_count > max_links:
                            break
                else:
                    print("No room left in queue; skipping cites for " + current.handle)

                print("Adding " + current.handle + " to database")
                db_session.add(article)

                if not len(current.citation_chain) == 0:
                    print("Adding citation chain for " + current.handle + " to database")
                    cite_chain = db.CitationChain(id_of_citing=article_counter,
                                                  citation_chain=Ltree(build_ltree(current.citation_chain + [article_counter])))
                    db_session.add(cite_chain)

                db_session.commit()

                # Once appended, add current handle to list of visited handles
                visited_handles.append(current.handle)
                article_counter += 1

            except AttributeError:
                print("No RePeC data for " + current.handle)

            except json.decoder.JSONDecodeError:
                print("Problem decoding JSON for " + current.handle + ". Skipping this one.")

            except NoDataException:
                print("Data missing for " + current.handle)

        # If the handle is already in the list of visited handles, then we need to add the current
        # citation chain to the citations table but will skip adding the article. To do this, we need
        # to query the database to get the id of the article itself
        elif len(current.citation_chain) == 0:
            article_id = db_session.query(db.Article).filter_by(handle=current.handle).scalar().id
            cite_chain = db.CitationChain(id_of_citing=article_id,
                                          citation_chain=Ltree(build_ltree(current.citation_chain + [article_counter])))
            db_session.add(cite_chain)
            db_session.commit()


