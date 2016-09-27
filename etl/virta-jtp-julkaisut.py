#!/usr/bin/python

from time import localtime, strftime
import time

import sys, os

import httplib
sourcehostname = "raja-dev.csc.fi"
httpconn = httplib.HTTPSConnection(sourcehostname)
#httpconn = httplib.HTTPConnection(sourcehostname)

import json

import pymssql
server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

def main():
  pyalku = time.time()
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

  print ("Connecting to database\n ->%s" % (database)).encode('utf-8')
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan SA_VIRTA_JTP_JULKAISUT").encode('utf-8')
  cur.execute("DELETE FROM SA_VIRTA_JTP_JULKAISUT")
  conn.commit()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" haetaan %s" % (sourcehostname)).encode('utf-8')
  apiuri = "/api/julkaisut"
  httpconn.request('GET', apiuri)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  lkm = 0
  for i in j:
    lkm += 1
    # sarakkeet
    julkaisunTunnus = getattr(i, "julkaisunTunnus", None)
    julkaisunNimi = getattr(i, "julkaisunNimi", None)
    tekijat = getattr(i, "tekijat", None)
    julkaisuVuosi = getattr(i, "julkaisuVuosi", None)
    julkaisuTyyppi = getattr(i, "julkaisuTyyppi", None)
    lehdenNimi = getattr(i, "lehdenNimi", None)
    kustantajanNimi = getattr(i, "kustantajanNimi", None)
    isbn = getattr(i, "isbn", None)
    issn = getattr(i, "issn", None)
    muutospvm = getattr(i, "muutospvm", None)
    luontipvm = getattr(i, "luontipvm", None)
    julkaisunTila = getattr(i, "julkaisunTila", None)
    doi = getattr(i, "doi", None)
    julkaisunOrgTunnus = getattr(i, "julkaisunOrgTunnus", None)
    yhteisJulkaisunTunnus = getattr(i, "yhteisJulkaisunTunnus", None)
    jufoTunnus = getattr(i, "jufoTunnus", None)
    organisaatioTunnus = getattr(i, "organisaatioTunnus", None)
    ilmoitusVuosi = getattr(i, "ilmoitusVuosi", None)
    
    if lkm%1000 == 0:
      print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d" % (lkm)).encode('utf-8')
    cur.execute("""INSERT INTO SA_VIRTA_JTP_JULKAISUT (julkaisunTunnus, julkaisunNimi, tekijat, julkaisuVuosi, julkaisuTyyppi, lehdenNimi, kustantajanNimi, isbn, issn, julkaisunTila, doi, julkaisunOrgTunnus, yhteisJulkaisunTunnus, jufoTunnus, organisaatioTunnus, ilmoitusVuosi) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s)""", (julkaisunTunnus, julkaisunNimi, tekijat, julkaisuVuosi, julkaisuTyyppi, lehdenNimi, kustantajanNimi, isbn, issn, julkaisunTila, doi, julkaisunOrgTunnus, yhteisJulkaisunTunnus, jufoTunnus, organisaatioTunnus, ilmoitusVuosi))
    conn.commit()

  cur.close()
  conn.close()

  pyloppu = time.time()
  pykesto = pyloppu - pyalku
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')
  print ("ajo kesti %f"%(pykesto)).encode('utf-8')

if __name__ == "__main__":
  main()
