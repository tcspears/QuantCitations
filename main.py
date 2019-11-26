from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import cache
import db
import scraper
import settings

engine = create_engine('postgresql://taylor:FurisJex22@localhost/quantcites', echo=False)
db.Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

cache = cache.DataCache()


seed_papers = {"cir":"RePEc:ecm:emetrp:v:53:y:1985:i:2:p:385-407",
               "holee":"RePEc:bla:jfinan:v:41:y:1986:i:5:p:1011-29",
               "vasicek":"RePEc:eee:jfinec:v:5:y:1977:i:2:p:177-188",
               "hjm":"RePEc:ecm:emetrp:v:60:y:1992:i:1:p:77-105",
               "dothan":"RePEc:eee:jfinec:v:6:y:1978:i:1:p:59-69",
               "brennan-schwartz-2":"RePEc:eee:jbfina:v:3:y:1979:i:2:p:133-155",
               "vasicek-fong":"RePEc:bla:jfinan:v:37:y:1982:i:2:p:339-48",
               "nelson-siegel":"RePEc:ucp:jnlbus:v:60:y:1987:i:4:p:473-89",
               "longstaff-schwartz":"RePEc:bla:jfinan:v:47:y:1992:i:4:p:1259-82"}

seed_handles = list(seed_papers.values())

# seed_handles = ["RePEc:eee:jetheo:v:20:y:1979:i:3:p:381-408"]

scraper.repec_scraper(db_session=session,
                      cache=cache,
                      seed_handles=seed_handles,
                      max_links=settings.MAX_LINKS)



