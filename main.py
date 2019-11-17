import library

cache = library.DataCache("/Users/taylor/QuantCitesDataCache/", wait_time=2)
conn = library.DBConnection(dbname="quantcites", user="taylor", password="FurisJex22", host="127.0.0.1", port="5432")

# Run spidering algorithm
seed_papers = {"cir":"RePEc:ecm:emetrp:v:53:y:1985:i:2:p:385-407",
               "holee":"RePEc:bla:jfinan:v:41:y:1986:i:5:p:1011-29",
               "vasicek":"RePEc:eee:jfinec:v:5:y:1977:i:2:p:177-188",
               "hjm":"RePEc:ecm:emetrp:v:60:y:1992:i:1:p:77-105",
               "dothan":"RePEc:eee:jfinec:v:6:y:1978:i:1:p:59-69",
               "brennan-schwartz":"RePEc:eee:jfinec:v:5:y:1977:i:1:p:67-88",
               "brennan-schwartz-2":"RePEc:eee:jbfina:v:3:y:1979:i:2:p:133-155",
               "hull-white":"RePEc:oup:rfinst:v:3:y:1990:i:4:p:573-92",
               "duffie-kan":"RePEc:bla:mathfi:v:6:y:1996:i:4:p:379-406",
               "brace-gatrek-musiela":"RePEc:bla:mathfi:v:7:y:1997:i:2:p:127-155",
               "miltersen-sandmann-sondermann":"RePEc:bla:jfinan:v:52:y:1997:i:1:p:409-30",
               "hagan-woodward":"RePEc:taf:apmtfi:v:6:y:1999:i:4:p:233-260",
               "bradley-crane":"RePEc:inm:ormnsc:v:19:y:1972:i:2:p:139-151",
               "vasicek-fong":"RePEc:bla:jfinan:v:37:y:1982:i:2:p:339-48",
               "nelson-siegel":"RePEc:ucp:jnlbus:v:60:y:1987:i:4:p:473-89",
               "longstaff-schwartz":"RePEc:bla:jfinan:v:47:y:1992:i:4:p:1259-82"}

seed_handles = list(seed_papers.values())

library.spidering_algorithm(db_conn=conn,
                            cache=cache,
                            seed_handles=seed_handles,
                            articles_table="articles",
                            citations_table="citations",
                            max_links=50000)