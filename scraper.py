import json
from collections import namedtuple
import dateparser
from bs4 import BeautifulSoup
import logging
from sqlalchemy_utils import Ltree
import persistqueue
import db
import settings

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s : %(name)s : %(levelname)s : %(message)s',
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
    article = dict()
    article["handle"] = repec_handle

    for key in json_text:
        for inner_key in key:
            if inner_key == '@id' and key[inner_key] == "#periodical":
                article['venue'] = key['name'].strip().lower()
            elif inner_key == '@id' and key[inner_key] == '#number':
                article['year'] = dateparser.parse(key['datePublished']).year
            elif inner_key == '@id' and key[inner_key] == '#article':
                article['title'] = key['name'].strip()
                article['authors'] = [author.strip().lower() for author in key['author'].split('&')]
                article['url'] = key['url'].strip()
                article['abstract'] = key['description'].strip()
                article['keywords'] = [keyword.strip().lower() for keyword in key['keywords'].split(';')]

    return article


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


def write_article(extracted_article, db_session):
    article_entry = db.Article(handle=extracted_article["handle"],
                               title=extracted_article["title"],
                               year=extracted_article["year"],
                               abstract=extracted_article["abstract"],
                               url=extracted_article["url"])

    # Check if venue exists; if it doesn't, add a new venue entry
    venue_entry = db_session.query(db.Venue).filter_by(name=extracted_article["venue"]).first()
    if venue_entry is None:
        venue_entry = db.Venue(name=extracted_article["venue"])
    article_entry.venue = venue_entry

    # Check if author(s) exist; if so, add them to the article. If not, create new author entries
    for author in extracted_article["authors"]:
        author_entry = db_session.query(db.Author).filter_by(name=author).first()
        if author_entry is None:
            author_entry = db.Author(name=author)
        article_entry.authors.append(author_entry)

    # Check if keyword(s) exist; if so, add them to article. If not, create new keyword entries
    for keyword in extracted_article["keywords"]:
        keyword_entry = db_session.query(db.Keyword).filter_by(keyword=keyword).first()
        if keyword_entry is None:
            keyword_entry = db.Keyword(keyword=keyword)
        article_entry.keywords.append(keyword_entry)

    logging.info("Adding " + str(article_entry) + " to database")
    db_session.add(article_entry)
    db_session.commit()


# Spiders RePEC and Citec from a list of seed papers
def repec_scraper(db_session,
                  cache,
                  seed_handles,
                  max_links=100,
                  persist_at=settings.CACHE_LOCATION):
    def build_ltree(citation_list):
        return ".".join(list(map(str, citation_list)))

    if not persist_at.endswith("/"):
        persist_at = persist_at + "/"

    # Initialize Queue object to store RePEC handles; fill it with seed handles.
    repec_queue = persistqueue.UniqueQ(persist_at + "scraper_queue", auto_commit=True)

    # Step 1: Check if articles db is empty. If it is, then we need to make sure that the
    # scraping queue is empty too.
    article_count = db.latest_article_id(db_session)
    articles_db_is_empty = article_count == 0

    # Clear the shelf and queue
    if articles_db_is_empty:
        logging.warning("Articles table is empty, so I'm clearing the repec_queue shelf")
        while not repec_queue.empty():
            repec_queue.get(timeout=0)

    # Initiate counter for article entries and link count
    link_count = len(repec_queue) + article_count

    # Add seed handles to the queue if they haven't been visited previously
    logging.info("Adding seed handles to queue...")
    for seed_handle in seed_handles:
        existing_entry = db_session.query(db.Article).filter_by(handle=seed_handle).first()
        # Because these articles are at the 'root' of the citation chain, the chain list is empty
        if existing_entry is None:
            repec_queue.put(ArticleInfo(seed_handle, []))
            link_count += 1

    # Spider through the queue
    while not repec_queue.empty():
        current = repec_queue.get(timeout=0)
        existing_entry = db_session.query(db.Article).filter_by(handle=current.handle).scalar()
        if existing_entry is None:
            try:
                # Download RePEC data and add current counter value as article ID
                logging.info("Getting RePEC data for " + current.handle)
                article_info = get_repec_data(cache, current.handle)
                write_article(article_info, db_session)
                latest_article_id = db.latest_article_id(db_session)

                if link_count < max_links:
                    # If we are below max_links, then get citec cites and add them to the queue
                    logging.info("Getting cites for " + current.handle)
                    cites = get_citec_cites(cache, current.handle)
                    for handle in cites:
                        # Second part takes current citation chain and appends current link counter onto it:
                        # e.g., [1,2] -> [1,2,3].
                        to_put = ArticleInfo(handle, current.citation_chain + [latest_article_id])
                        repec_queue.put(to_put)
                        logging.info("Current value of link_count : " + str(link_count))
                        link_count += 1
                        if link_count > max_links:
                            break
                else:
                    logging.info("No room left in queue; skipping cites for " + current.handle)

                if len(current.citation_chain) != 0:
                    logging.info("Adding citation chain for " + current.handle + " to database")
                    cite_chain = db.CitationChain(id_of_citing=latest_article_id,
                                                  citation_chain=Ltree(
                                                      build_ltree(current.citation_chain + [latest_article_id])))
                    db_session.add(cite_chain)
                    db_session.commit()

            except AttributeError:
                logging.warning("No RePeC data for " + current.handle)

            except json.decoder.JSONDecodeError:
                logging.error("Problem decoding JSON for " + current.handle + ". Skipping this one.")

            except NoDataException:
                logging.warning("CitEc data missing for " + current.handle)

        # If the handle is already in the list of visited handles, then we need to add the current
        # citation chain to the citations table but will skip adding the article. To do this, we need
        # to query the database to get the id of the article itself

        elif len(current.citation_chain) != 0:
            logging.info("Article " + current.handle + " already exists. Adding an additional citation chain.")
            cite_chain = db.CitationChain(id_of_citing=existing_entry.id,
                                          citation_chain=Ltree(
                                              build_ltree(current.citation_chain + [existing_entry.id])))
            db_session.add(cite_chain)
            db_session.commit()

        else:
            logging.info(
                "Article " + current.handle + " already exists, but citation chain is empty. So I'm skipping to the next.")
