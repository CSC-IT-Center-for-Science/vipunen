#!/usr/bin/python

from time import localtime, strftime
import sys, os

import httplib
sourcehostname = "dwitjutife1.csc.fi"
httpconn = httplib.HTTPSConnection(sourcehostname)
#httpconn = httplib.HTTPConnection(sourcehostname)

import json

import pymssql
server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

# hae avaimen arvo json:sta 
def jv(jsondata, key):
  if key in jsondata:
    return jsondata[key]
  return None

def main():
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

  #print ("Connecting to database %s" % (database)).encode('utf-8')
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan sa_virta_jtp_julkaisut").encode('utf-8')
  cur.execute("DELETE FROM sa_virta_jtp_julkaisut")
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
    julkaisunTunnus = jv(i ,"julkaisunTunnus")
    julkaisunNimi = jv(i, "julkaisunNimi")
    tekijat = jv(i, "tekijat")
    julkaisuVuosi = jv(i, "julkaisuVuosi")
    julkaisuTyyppi = jv(i, "julkaisuTyyppi")
    lehdenNimi = jv(i, "lehdenNimi")
    kustantajanNimi = jv(i, "kustantajanNimi")
    isbn = jv(i, "isbn")
    issn = jv(i, "issn")
    muutospvm = jv(i, "muutospvm")
    luontipvm = jv(i, "luontipvm")
    julkaisunTila = jv(i, "julkaisunTila")
    doi = jv(i, "doi")
    julkaisunOrgTunnus = jv(i, "julkaisunOrgTunnus")
    yhteisJulkaisunTunnus = jv(i, "yhteisJulkaisunTunnus")
    jufoTunnus = jv(i, "jufoTunnus")
    organisaatioTunnus = jv(i, "organisaatioTunnus")
    ilmoitusVuosi = jv(i, "ilmoitusVuosi")
    
    #if lkm%1000 == 0:
    #  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d" % (lkm)).encode('utf-8')
    cur.execute("""INSERT INTO sa_virta_jtp_julkaisut (julkaisunTunnus, julkaisunNimi, tekijat, julkaisuVuosi, julkaisuTyyppi, lehdenNimi, kustantajanNimi, isbn, issn, julkaisunTila, doi, julkaisunOrgTunnus, yhteisJulkaisunTunnus, jufoTunnus, organisaatioTunnus, ilmoitusVuosi) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s)""", (julkaisunTunnus, julkaisunNimi, tekijat, julkaisuVuosi, julkaisuTyyppi, lehdenNimi, kustantajanNimi, isbn, issn, julkaisunTila, doi, julkaisunOrgTunnus, yhteisJulkaisunTunnus, jufoTunnus, organisaatioTunnus, ilmoitusVuosi))
    conn.commit()

  cur.close()
  conn.close()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
  main()
