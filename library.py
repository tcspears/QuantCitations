import codecs
import glob
import json
import queue
import time
from collections import namedtuple
import dateparser
import psycopg2 as ps
import requests
from bs4 import BeautifulSoup

import settings


class NoDataException(Exception):
    pass


class DBConnection:
    def __init__(self, **kwargs):
        self.connection = ps.connect(**kwargs)

    # Takes a SELECT SQL query and executes it
    def sql_fetch(self, sql_statement):
        contains_select_keyword = "select" in sql_statement.lower()
        if not contains_select_keyword:
            raise Exception("SQL query isn't a SELECT query")
        cursor = self.connection.cursor()
        cursor.execute(sql_statement)
        return cursor.fetchall()

    # Takes one or more INSERT SQL queries and executes them
    def sql_put(self, sql_statements):
        if not isinstance(sql_statements, list):
            sql_statements = [sql_statements]

        contains_insert_keyword = all({"insert" in statement.lower() for statement in sql_statements})
        if not contains_insert_keyword:
            raise Exception("SQL query isn't an INSERT query")

        cursor = self.connection.cursor()
        for statement in sql_statements:
            cursor.execute(statement)
        self.connection.commit()

    def get_all_handles(self, articles_table="articles"):
        sql_output = self.sql_fetch("SELECT handle FROM " + articles_table)
        return [i[0] for i in sql_output]



class DataCache:
    def __init__(self, cache_location, wait_time=2):
        self.cache_location = cache_location
        self.wait_time = wait_time
        self.repec_list = glob.glob(cache_location + "repec/" + "*")
        self.citec_list = glob.glob(cache_location + "citec/" + "*")

    def _build_handle_from_filename(self, filename):
        return filename.replace("_", "/").split(".")[0]

    def request_repec(self, handle):
        def build_file_path(filename):
            return self.cache_location + "repec/" + filename

        corresp_file = handle.replace("/", "_") + ".html"

        if build_file_path(corresp_file) in self.repec_list:
            file = codecs.open(build_file_path(corresp_file), 'r')
            return file.read()
        else:
            request = requests.get("https://ideas.repec.org/cgi-bin/h.cgi?h=" + handle)
            file = open(build_file_path(corresp_file), 'w')
            file.write(request.text)
            file.close()
            self.repec_list.append(corresp_file)
            time.sleep(self.wait_time)
            return request.text

    def request_citec(self, handle):
        def build_file_path(filename):
            return self.cache_location + "citec/" + filename

        corresp_file = handle.replace("/", "_") + ".xml"

        if build_file_path(corresp_file) in self.citec_list:
            file = codecs.open(build_file_path(corresp_file), 'r')
            return file.read()
        else:
            request = requests.get("http://citec.repec.org/api/citedby/" + handle + "/" + settings.CITEC_USERNAME)
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
    items = dict()

    for key in json_text:
        for inner_key in key:
            if inner_key == '@id' and key[inner_key] == "#periodical":
                items['pub_venue'] = key['name'].strip()
            elif inner_key == '@id' and key[inner_key] == '#number':
                items['pub_year'] = dateparser.parse(key['datePublished']).year
            elif inner_key == '@id' and key[inner_key] == '#article':
                items['title'] = key['name'].strip()
                items['authors'] = [author.strip() for author in key['author'].split('&')]
                items['url'] = key['url'].strip()
                items['abstract'] = key['description'].strip()
                items['keywords'] = [keyword.strip() for keyword in key['keywords'].split(';')]

    article = Article(handle=repec_handle,
                      title=items['title'],
                      pub_year=items['pub_year'],
                      pub_venue=items['pub_venue'],
                      authors=items['authors'],
                      abstract=items['abstract'],
                      url=items['url'],
                      keywords=items['keywords'])
    return (article)


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


# Converts an article object into a SQL statement to insert that article into the database
def construct_insertion_sql(article, cites, articles_table="articles", citations_table="citations"):
    def sanitize_string(input_string):
        return (input_string.replace("'", "''"))

    def construct_array_portion(input_list):
        if len(input_list) == 0:
            return ("'{}'")
        else:
            output = "'{"
            for element in input_list[:-1]:
                output = output + "\"" + sanitize_string(element) + "\", "
            output = output + "\"" + sanitize_string(input_list[-1]) + "\"}'"
            return output

    def construct_citing_sql(citing_handle, cited_handle, citations_table):
        cit_prefix = "INSERT INTO " + citations_table + " (citing_handle, cited_handle) VALUES ("
        citing_handle = "'" + citing_handle + "', "
        cited_handle = "'" + cited_handle + "'"
        cit_suffix = ")"
        cit_sql = cit_prefix + citing_handle + cited_handle + cit_suffix
        return (cit_sql)

    # Build article insertion SQL. Results in one SQL statement.
    art_prefix = "INSERT INTO " + articles_table + " (handle, title, pub_year, pub_venue, authors, abstract, url, keywords) VALUES ("
    handle = "'" + article.handle + "', "
    title = "'" + sanitize_string(article.title) + "', "
    pub_year = str(article.pub_year) + ", "
    pub_venue = "'" + sanitize_string(article.pub_venue) + "', "
    authors = construct_array_portion(article.authors) + ", "
    abstract = "'" + sanitize_string(article.abstract) + "', "
    url = "'" + article.url + "', "
    keywords = construct_array_portion(article.keywords)
    art_suffix = ")"
    article_sql = art_prefix + handle + title + pub_year + pub_venue + authors + abstract + url + keywords + art_suffix

    # Build citation SQL
    citation_sql = [construct_citing_sql(citing_handle=element,
                                         cited_handle=article.handle,
                                         citations_table=citations_table) for element in cites]

    sql_output = list()
    sql_output.append(article_sql)
    sql_output.extend(citation_sql)

    return sql_output


# Spiders RePEC and Citec from a list of seed papers
def spidering_algorithm(db_conn,
                        cache,
                        seed_handles,
                        articles_table="articles",
                        citations_table="citations",
                        max_links=100,
                        wait_time=2):
    # Initialize Queue object to store RePEC handles; fill it with seed handles
    repec_queue = queue.Queue()
    visited_handles = list()

    for handle in seed_handles:
        repec_queue.put(handle)

    link_count = 0

    # Spider through the queue
    while not repec_queue.empty() and link_count < max_links:
        current = repec_queue.get()

        if current not in visited_handles:

            try:
                # Download RePEC data and add it to the database
                print("Getting RePEC data for " + current)
                article = get_repec_data(cache, current)

                # Get citec cites and add them to the queue
                print("Getting cites for " + current)
                cites = get_citec_cites(cache, current)
                for handle in cites:
                    repec_queue.put(handle)

                article_sql = construct_insertion_sql(article, cites, articles_table, citations_table)
                db_conn.sql_put(article_sql)
                print("Adding " + current + " to database")
                link_count += 1

                # Once appended, add current handle to list of visited handles
                visited_handles.append(current)

            except AttributeError:
                print("No RePeC data for " + current)

            except json.decoder.JSONDecodeError:
                print("Problem decoding JSON for " + current + ". Skipping this one.")

            except NoDataException:
                print("Data missing for " + current)


