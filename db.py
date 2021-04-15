from sqlalchemy import Column, Integer, String, BigInteger, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKey, Table
from sqlalchemy_utils import LtreeType
from sqlalchemy import Index
from sqlalchemy.orm import relationship, remote, foreign, backref
from sqlalchemy import func
from sqlalchemy.sql.expression import cast, text

Base = declarative_base()

# TODO: Update Database definition to allow for longer abstracts (ideally of arbitrary size)
# TODO: Develop Author/Venue tables?

author_article_association = Table("author_article_association",
                                   Base.metadata,
                                   Column("author_id", BigInteger, ForeignKey("authors.id")),
                                   Column("article_id", BigInteger, ForeignKey("articles.id"))
                                   )

keyword_article_association = Table("keyword_article_association",
                                    Base.metadata,
                                    Column("keyword_id", BigInteger, ForeignKey("keywords.id")),
                                    Column("article_id", BigInteger, ForeignKey("articles.id"))
                                    )

class Venue(Base):
    __tablename__ = 'venues'

    id = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    articles = relationship(
        "Article",
        back_populates="venue"
    )

    def __str__(self):
        return '{}'.format(self.name)

    def __repr__(self):
        return 'Venue({})'.format(self.name)


class Article(Base):
    __tablename__ = 'articles'

    id = Column(BigInteger, primary_key=True)
    handle = Column(String(), nullable=False, unique=True)
    title = Column(String())
    year = Column(Integer)
    venue_id = Column(BigInteger, ForeignKey("venues.id"))
    abstract = Column(String())
    url = Column(String())
    keywords = relationship(
        "Keyword",
        secondary=keyword_article_association,
        back_populates="articles"
    )
    authors = relationship(
        "Author",
        secondary=author_article_association,
        back_populates="articles"
    )
    citation_chains = relationship(
        "CitationChain",
        back_populates="corresp_article"
    )
    venue = relationship(
        "Venue",
        back_populates="articles"
    )

    def __str__(self):
        return '{}'.format(self.handle)

    def __repr__(self):
        return 'Article({})'.format(self.handle)


class Keyword(Base):
    __tablename__ = 'keywords'

    id = Column(BigInteger, primary_key=True)
    keyword = Column(String, nullable=False)
    articles = relationship(
        "Article",
        secondary=keyword_article_association,
        back_populates="keywords"
    )

    def __str__(self):
        return '{}'.format(self.keyword)

    def __repr__(self):
        return 'Keyword({})'.format(self.keyword)


class Author(Base):
    __tablename__ = 'authors'

    id = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    articles = relationship(
        "Article",
        secondary=author_article_association,
        back_populates="authors"
    )

    def __str__(self):
        return '{}'.format(self.name)

    def __repr__(self):
        return 'Author({})'.format(self.name)


class CitationChain(Base):
    __tablename__ = 'citations'

    id_of_citing = Column(BigInteger, ForeignKey("articles.id"), nullable=False)
    citation_chain = Column(LtreeType, primary_key=True)
    corresp_article = relationship(
        'Article',
        back_populates="citation_chains"
    )
    chain_origin = relationship(
        'Article',
        primaryjoin=remote(Article.id) == cast(foreign(func.ltree2text(func.subltree(citation_chain, 0, 1))), Integer),
        backref='chain_descendents',
        viewonly=True
    )

    __tableargs__ = (
        Index('ix_citations_citation_chain', citation_chain, postgresql_using="gist")
    )

    def __str__(self):
        return 'CitationChain(' + str(self.corresp_article) + ": " + str(self.citation_chain) + ")"

    def __repr__(self):
        return 'CitationChain(' + str(self.corresp_article) + ": " + str(self.citation_chain) + ")"


def latest_article_id(session):
    latest_entry = session.query(Article).order_by(Article.id.desc()).first()
    if latest_entry is None:
        return 0
    else:
        return latest_entry.id
