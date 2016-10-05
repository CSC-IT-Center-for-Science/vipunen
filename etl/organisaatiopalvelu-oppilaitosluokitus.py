#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,os
import httplib
import pymssql
import json
from time import localtime, strftime

hostname = "virkailija.opintopolku.fi"
links = [
    "/organisaatio-service/rest/organisaatio/v2/hierarkia/hae?organisaatiotyyppi=Koulutustoimija&aktiiviset=true&suunnitellut=true&lakkautetut=false",
    "/organisaatio-service/rest/organisaatio/v2/hierarkia/hae?organisaatiotyyppi=Koulutustoimija&aktiiviset=false&suunnitellut=false&lakkautetut=true"
]
table = "sa_oppilaitosluokitus"

server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

def main():
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"

  #print "Connecting to database %s" % (database)
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor(as_dict=True)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" delete %s" % (table)
  cur.execute("DELETE FROM [%s]"%(table))
  conn.commit()

  httpconn = httplib.HTTPSConnection(hostname)
  
  for url in links:
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url
    httpconn.request('GET', url)
    r = httpconn.getresponse()
    j = json.loads(r.read())
    cnt = 0
    for i in j["organisaatiot"]:
      jarjestajaoid = i["oid"]
      jarjestajakoodi = None if "ytunnus" not in i else i["ytunnus"] # voi olla None
      jarjestajanimi = None if "fi" not in i["nimi"] else i["nimi"]["fi"]
      jarjestajanimi_sv = None if "sv" not in i["nimi"] else i["nimi"]["sv"]
      jarjestajanimi_en = None if "en" not in i["nimi"] else i["nimi"]["en"]

      for o in i["children"]:
        if "oppilaitosKoodi" in o:
          cnt += 1
          # sarakkeet
          oid = ""
          koodi = ""
          nimi = ""
          nimi_sv = None
          nimi_en = None
          alkupvm = ""
          loppupvm = None
          kuntakoodi = None # nimet erikseen
          oppilaitostyyppikoodi = None # nimet erikseen
          
          oid = o["oid"]
          koodi = o["oppilaitosKoodi"]
          nimi = None if "fi" not in o["nimi"] else o["nimi"]["fi"]
          nimi_sv = None if "sv" not in o["nimi"] else o["nimi"]["sv"]
          nimi_en = None if "en" not in o["nimi"] else o["nimi"]["en"]
          alkupvm = "1900-1-1" if "alkuPvm" not in o or o["alkuPvm"]<0 else strftime("%Y-%m-%d",localtime(o["alkuPvm"]/1000))
          loppupvm = None if "lakkautusPvm" not in o else strftime("%Y-%m-%d",localtime(o["lakkautusPvm"]/1000))
          
          kuntakoodi = o["kotipaikkaUri"].replace("kunta_","")
          # => nimitiedot erikseen
          oppilaitostyyppikoodi = o["oppilaitostyyppi"].replace("oppilaitostyyppi_","").replace("#1","")
          # => nimitiedot erikseen
  
          #print strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d -- %s" % (cnt,koodi)
          cur.execute("INSERT INTO [%s]"%(table)+"""
          (oid,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm,kuntakoodi,oppilaitostyyppikoodi,jarjestajaoid,jarjestajakoodi,jarjestajanimi,jarjestajanimi_sv,jarjestajanimi_en ,source)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s ,%s)
          """, (oid,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm,kuntakoodi,oppilaitostyyppikoodi,jarjestajaoid,jarjestajakoodi,jarjestajanimi,jarjestajanimi_sv,jarjestajanimi_en ,hostname+url))
          conn.commit()

  cur.close()
  conn.close()
  
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"
    
if __name__ == "__main__":
    main()
