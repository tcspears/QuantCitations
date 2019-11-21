from sqlalchemy import Column, Integer, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import ARRAY
from sqlalchemy.schema import ForeignKey
from sqlalchemy_utils import LtreeType
from sqlalchemy import Index
from sqlalchemy.orm import relationship, remote, foreign, backref
from sqlalchemy import func
from sqlalchemy.sql.expression import cast, text

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'

    id = Column(BigInteger, primary_key=True)
    handle = Column(String(length=300), nullable=False, unique=True)
    title = Column(String(length=2000))
    pub_year = Column(Integer)
    pub_venue = Column(String(length=500))
    authors = Column(ARRAY(String))
    abstract = Column(String(length=20000))
    url = Column(String(length=300))
    keywords = Column(ARRAY(String))

    def __str__(self):
        return 'Article({})'.format(self.handle)

    def __repr__(self):
        return 'Article({})'.format(self.handle)


class CitationChain(Base):
    __tablename__ = 'citations'

    id_of_citing = Column(BigInteger, ForeignKey("articles.id"), nullable=False)
    citation_chain = Column(LtreeType, primary_key=True)
    corresp_article = relationship(
        'Article',
        backref="citation_chain"
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
