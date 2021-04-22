import db
import pandas as pd
from functools import reduce
from sqlalchemy.sql import text

'''
Class that holds a DataFrame of Articles
'''
class ArticleCollection:
    def __init__(self, df):
        """
        :param df: A DataFrame object of correct dimension and column names
        """
        correct_columns = df.columns.to_list() == ['ID', 'Handle', 'Title', 'Year', 'Authors', 'Venue', 'URL', 'Abstract', 'Keywords']
        if not correct_columns:
            raise TypeError("Incorrect format for input DataFrame")
        self.articles_df = df

    def __len__(self):
        return len(self.articles_df)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.articles_df == other.articles_df

    def __str__(self):
        return self.articles_df.__str__()

    def __repr__(self):
        return self.articles_df.__repr__()

    def to_csv(self, file_name):
        """
        Output to CSV
        :param file_name: Path and file_name of .csv file
        :return: None
        """
        self.articles_df.to_csv(file_name)


def article_info(article):
    """
    Extract attributes of an Article object
    :param article: An Article object (e.g. returned via a lookup function)
    :return: A dict containing the Article's attributes
    """
    return {'ID': article.id,
            'Handle': article.handle,
            'Title': article.title,
            'Year': article.year,
            'Authors': [author.name for author in article.authors],
            'Venue': article.venue,
            'URL': article.url,
            'Abstract': article.abstract,
            'Keywords': [keyword.keyword for keyword in article.keywords]}


def lookup_by_handle(repec_handle, session_object):
    """
    Lookup an Article by its RePEC handle
    :param repec_handle: A string containing the Article's RePEC handle
    :param session_object: A SQLAlchemy session object
    :return: An Article object
    """
    return session_object.query(db.Article).filter_by(handle=repec_handle).scalar()


def extract_handles(article_collection):
    """
    Generates a set of RePEC handles from an ArticleCollection object
    :param article_collection: An instance of ArticleCollection
    :return: A set of RePEC handles
    """
    return set(article_collection.articles_df["Handle"].to_list())


def reduce_on_handles(f, article_collections):
    """
    Helper function to run set operations on ArticleCollection objects
    :param f: A binary function f(x,y) -> z where x, y, and z are sets objects
    :param article_collections: A list of ArticleCollection objects
    :return: A set object
    """
    article_sets = list(map(extract_handles, article_collections))
    return set(reduce(f, article_sets[1:], article_sets[0]))


def intersect_handles(article_collections):
    """
    Find set intersection of handles within a list of ArticleCollection objects
    :param article_collections: list of ArticleCollection objects
    :return: A set of handles
    """
    return reduce_on_handles(lambda x, y: x.intersection(y), article_collections)


def union_handles(article_collections):
    """
    Find the set union of handles within a list of ArticleCollection objects.
    :param article_collections: A list of ArticleCollection objects
    :return: set of handles
    """
    return reduce_on_handles(lambda x, y: x.union(y), article_collections)


def difference_handles(article_collections):
    """
    Find the set difference (i.e. complement) of a list of ArticleCollection objects
    Note: A-B-C = A-(BUC) -> Returns all elements of A not in B or C.
    :param article_collections: A list of ArticleCollection objects
    :return: set of handles
    """
    return reduce_on_handles(lambda x, y: x.difference(y), article_collections)


def combine(article_collections, handles_set=None):
    """
    Combine (i.e. combine) one or more ArticleCollection objects while dropping duplicates.
    If handles_set is specified, the resulting ArticleCollection object will only contain articles
    with the specified RePEC handles.
    :param article_collections: A list of ArticleCollection objects
    :param handles_set: An optional set of RePEC handles
    :return: An ArticleCollection object
    """
    dfs = map(lambda x: x.articles_df, article_collections)
    new = pd.concat(dfs).drop_duplicates(subset=["Handle"]).reset_index(drop=True)
    if handles_set is not None:
        new = new[new['Handle'].isin(list(handles_set))]
    return ArticleCollection(new)


def shared_handles_percent(article_collections):
    """
    Calculate the percentage of shared handles out of the total handles included in a list of ArticleCollection objects.
    :param article_collections: A list of ArticleCollection objects
    :return: A float
    """
    numerator = len(intersect_handles(article_collections))
    denominator = len(union_handles(article_collections))
    return numerator/denominator


def get_descendants(article, session_object, degree=(1,1)):
    """
    Create an ArticleCollection of the citation descendants of an article
    :param article: An Article object (returned by a lookup function)
    :param session_object: A SQLAlchemy session object
    :param degree: Number of generations to query. If left unspecified, all descendants are returned.
    :return: ArticleCollection
    """
    if degree is None:
        end_part = ".*"
    if degree[0] == degree[1]:
        end_part = ".*{" + str(degree[0]) + "}"
    else:
        end_part = ".*{" + str(degree[0]) + "," + str(degree[1]) + "}"
    id = article.id
    citation_chain_query = "*." + str(id) + end_part
    sql_statement = text("select articles.id, articles.handle, articles.title, articles.year, "
                         "string_agg(distinct authors.name, \'; \'), venues.name, articles.url, articles.abstract, "
                         "string_agg(distinct keywords.keyword, \'; \') "
                         "from citations "
                         "left join articles on citations.id_of_citing = articles.id "
                         "left join venues on articles.venue_id = venues.id "
                         "left join author_article_association on articles.id = author_article_association.article_id "
                         "left join authors on author_article_association.author_id = authors.id "
                         "left join keyword_article_association on articles.id = keyword_article_association.article_id "
                         "left join keywords on keyword_article_association.keyword_id = keywords.id "
                         "where citation_chain ~ :q group by articles.id, venues.name;").params(q=citation_chain_query)
    sql_output = session_object.execute(sql_statement)
    df = pd.DataFrame(sql_output, columns = ['ID', 'Handle', 'Title', 'Year', 'Authors', 'Venue', 'URL', 'Abstract', 'Keywords'])
    return ArticleCollection(df)


def subset_rates_articles(article_collection):
    data = article_collection.articles_df
    rates_terms = ["interest rate model", "libor", "euribor", "term structure",
                   "yield curve", "zero coupon", "discount curve", "bond",
                   "treasury", "gilt", "bund", "short rate", "eonia", "sonia",
                   "fed funds", "convexity", "duration", "maturity", "maturities",
                   "caplet", "cap", "swaption", "term premia", "term premium",
                   "forward rate", "yield", "cms", "market price of risk"]
    terms_with_spaces = [" " + term + " " for term in rates_terms]
    data_subset = data[data.Abstract.str.contains("|".join(terms_with_spaces), case=False)]
    return ArticleCollection(data_subset)


def get_venue(venue_name, session_object):
    """
    Return a list of venues whose name field contains venue_name
    :param venue_name: A string giving the venue's name (e.g. Journal of Finance)
    :param session_object: A SQLAlchemy session object
    :return: A list of Venue objects
    """
    return session_object.query(db.Venue).filter(db.Venue.name.contains(venue_name)).all()


def build_abstracts_df(session_object):
    """
    Return a list of all abstracts in the database
    :param session_object: A SQLAlchemy session object
    :return: A list of strings
    """
    all_articles = session_object.query(Article).all()
    abstracts = [article.abstract for article in all_articles]
    handles = [article.handle for article in all_articles]
    years = [article.year for article in all_articles]
    return pd.DataFrame({'Handle': handles, 'Year': years, 'Abstract': abstracts})
