from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
import cache
import db
import scraper
import settings
import email_notifier

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s : %(name)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    handlers=[
                        logging.FileHandler("{0}/{1}.log".format(settings.LOG_PATH, settings.LOG_FILE)),
                        logging.StreamHandler()
                    ])

engine = create_engine('postgresql://taylor:FurisJex22@localhost/quantcites', echo=False)
db.Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

cache = cache.DataCache()

"""
seed_papers = {"cir":"RePEc:ecm:emetrp:v:53:y:1985:i:2:p:385-407",
               "holee":"RePEc:bla:jfinan:v:41:y:1986:i:5:p:1011-29",
               "vasicek":"RePEc:eee:jfinec:v:5:y:1977:i:2:p:177-188",
               "hjm":"RePEc:ecm:emetrp:v:60:y:1992:i:1:p:77-105",
               "dothan":"RePEc:eee:jfinec:v:6:y:1978:i:1:p:59-69",
               "brennan-schwartz-2":"RePEc:eee:jbfina:v:3:y:1979:i:2:p:133-155",
               "vasicek-fong":"RePEc:bla:jfinan:v:37:y:1982:i:2:p:339-48",
               "nelson-siegel":"RePEc:ucp:jnlbus:v:60:y:1987:i:4:p:473-89",
               "longstaff-schwartz":"RePEc:bla:jfinan:v:47:y:1992:i:4:p:1259-82",
               "bgm":"RePEc:bla:mathfi:v:7:y:1997:i:2:p:127-155",
               "sondermann":"RePEc:bla:jfinan:v:52:y:1997:i:1:p:409-30",
               "jamshidian":"RePEc:spr:finsto:v:1:y:1997:i:4:p:293-330",''
               "cir2":"RePEc:ecm:emetrp:v:53:y:1985:i:2:p:363-84",
               "hunt-kennedy":"RePEc:spr:finsto:v:4:y:2000:i:4:p:391-408",
               "kim-wright":"RePEc:fip:fedgfe:2005-33",
               "cochrane-piazzesi":"RePEc:aea:aecrev:v:95:y:2005:i:1:p:138-160",
               "constantinides":"RePEc:oup:rfinst:v:5:y:1992:i:4:p:531-52",
               # Bradley-Crane below is the wrong RePEc handle. The real one is given in Bradley-Crane-Actual.
               "bradley-crane":"RePEc:eee:dyncon:v:22:y:1998:i:10:p:1517-1541",
               "elliot-echols":"RePEc:cup:jfinqa:v:11:y:1976:i:01:p:87-114_02",
               "barry-ayres":"RePEc:inm:ormnsc:v:26:y:1980:i:6:p:539-569",
               "McCulloch":"RePEc:ucp:jnlbus:v:44:y:1971:i:1:p:19-31",
               "McCulloch2":"RePEc:bla:jfinan:v:30:y:1975:i:3:p:811-30",
               'waggoner':'repec:fip:fedawp:97-10',
               'duffie-kan':'RePEc:bla:mathfi:v:6:y:1996:i:4:p:379-406',
               'bjork-christiansen':'RePEc:bla:mathfi:v:9:y:1999:i:4:p:323-348',
               'fisher':'RePEc:fip:fedgfe:95-1',
               'dai-singleton':'RePEc:bla:jfinan:v:55:y:2000:i:5:p:1943-1978',
               'bradley-crane-actual':'RePEc:inm:ormnsc:v:19:y:1972:i:2:p:139-151',
               'hull-white':'RePEc:cup:jfinqa:v:28:y:1993:i:02:p:235-254_00'
               }
"""
seed_papers = {'harrison-kreps':'RePEc:eee:jetheo:v:20:y:1979:i:3:p:381-408',
               'cox-ross':'RePEc:eee:jfinec:v:3:y:1976:i:1-2:p:145-166'}

seed_handles = list(seed_papers.values())

email_notifier.send_notification_email('Scraper activated with the following seed handles: ' + str(seed_handles))

try:
    scraper.repec_scraper(db_session=session,
                          cache=cache,
                          seed_handles=seed_handles,
                          max_links=settings.MAX_LINKS)
except Exception as e:
    print(str(e))
    email_notifier.send_notification_email('Scraper encountered an exception and has failed. Exception text: ' + str(e))

