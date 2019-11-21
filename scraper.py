import json
import queue
from collections import namedtuple
import dateparser
from bs4 import BeautifulSoup
import logging
from sqlalchemy_utils import Ltree

import db
import settings

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    handlers=[
                        logging.FileHandler("{0}/{1}.log".format(settings.LOG_PATH, settings.LOG_FILE)),
                        logging.StreamHandler()
                    ])


class NoDataException(Exception):
    pass


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


# Define a namedtuple that will keep track of elements in queue. This consists of a RePec handle
# and an int list defining a citation path to that handle (e.g. [1,10,12] for article with id=12.

ArticleInfo = namedtuple("ArticleInfo", "handle citation_chain")


# Spiders RePEC and Citec from a list of seed papers
def repec_scraper(db_session,
                  cache,
                  seed_handles,
                  max_links=100):

    def build_ltree(citation_list):
        return ".".join(list(map(str, citation_list)))


    # Initialize Queue object to store RePEC handles; fill it with seed handles.
    repec_queue = queue.Queue()
    visited_handles = list()

    for handle in seed_handles:
        # Because these articles are at the 'root' of the citation chain, the chain list is empty
        repec_queue.put(ArticleInfo(handle, []))

    # Initiate counter for article entries and link count
    article_counter = 1
    link_count = 0

    # Spider through the queue
    while not repec_queue.empty():

        current = repec_queue.get()

        if current.handle not in visited_handles:

            try:
                # Download RePEC data and add current counter value as article ID
                logging.info("Getting RePEC data for " + current.handle)
                article = get_repec_data(cache, current.handle)
                article.id = article_counter

                if link_count < max_links:
                    # If we are below max_links, then get citec cites and add them to the queue
                    logging.info("Getting cites for " + current.handle)
                    cites = get_citec_cites(cache, current.handle)
                    for handle in cites:
                        # Second part takes current citation chain and appends current link counter onto it:
                        # e.g., [1,2] -> [1,2,3].
                        to_put = ArticleInfo(handle, current.citation_chain + [article_counter])
                        repec_queue.put(to_put)
                        print("link count : " + str(link_count))
                        link_count += 1
                        if link_count > max_links:
                            break
                else:
                    logging.info("No room left in queue; skipping cites for " + current.handle)

                logging.info("Adding " + current.handle + " to database")
                db_session.add(article)

                if not len(current.citation_chain) == 0:
                    logging.info("Adding citation chain for " + current.handle + " to database")
                    cite_chain = db.CitationChain(id_of_citing=article_counter,
                                                  citation_chain=Ltree(build_ltree(current.citation_chain + [article_counter])))
                    db_session.add(cite_chain)

                db_session.commit()

                # Once appended, add current handle to list of visited handles
                visited_handles.append(current.handle)
                article_counter += 1

            except AttributeError:
                logging.warning("No RePeC data for " + current.handle)

            except json.decoder.JSONDecodeError:
                logging.error("Problem decoding JSON for " + current.handle + ". Skipping this one.")

            except NoDataException:
                logging.warning("Data missing for " + current.handle)

        # If the handle is already in the list of visited handles, then we need to add the current
        # citation chain to the citations table but will skip adding the article. To do this, we need
        # to query the database to get the id of the article itself
        elif len(current.citation_chain) == 0:
            article_id = db_session.query(db.Article).filter_by(handle=current.handle).scalar().id
            logging.info("Adding citation chain for " + current.handle + " to database")
            cite_chain = db.CitationChain(id_of_citing=article_id,
                                          citation_chain=Ltree(build_ltree(current.citation_chain + [article_counter])))
            db_session.add(cite_chain)
            db_session.commit()


