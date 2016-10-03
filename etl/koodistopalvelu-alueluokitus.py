#!/usr/bin/python

from time import localtime, strftime
import sys, os

import httplib
sourcehostname = "virkailija.opintopolku.fi"
httpconn = httplib.HTTPSConnection(sourcehostname)

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

def haenimi(i,kieli):
    for m in i["metadata"]:
        if m["kieli"] == kieli:
            return m["nimi"]
    return None

def main():
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

  #print ("Connecting to database %s" % (database)).encode('utf-8')
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan SA_ALUELUOKITUS").encode('utf-8')
  cur.execute("DELETE FROM SA_ALUELUOKITUS")
  conn.commit()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" haetaan opintopolusta").encode('utf-8')
  url = "/koodisto-service/rest/json/kunta/koodi"
  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  lkm = 0
  for i in j:
    lkm += 1
    # sarakkeet
    koodi = jv(i,"koodiArvo")
    nimi = haenimi(i,"FI")
    nimi_sv = haenimi(i,"SV")
    nimi_en = haenimi(i,"FI")
    alkupvm = jv(i,"voimassaAlkuPvm")
    loppupvm = jv(i,"voimassaLoppuPvm")
    maakuntakoodi = None
    maakuntanimi = None
    maakuntanimi_sv = None
    maakuntanimi_en = None
    avikoodi = None
    avinimi = None
    avinimi_sv = None
    avinimi_en = None
    elykoodi = None
    elynimi = None
    elynimi_sv = None
    elynimi_en = None
    kielisuhdekoodi = None
    kielisuhdenimi = None
    kielisuhdenimi_sv = None
    kielisuhdenimi_en = None
    seutukuntakoodi = None
    seutukuntanimi = None
    seutukuntanimi_sv = None
    seutukuntanimi_en = None
    laanikoodi = None
    laaninimi = None
    laaninimi_sv = None
    laaninimi_en = None
    kuntaryhmakoodi = None
    kuntaryhmanimi = None
    kuntaryhmanimi_sv = None
    kuntaryhmanimi_en = None

    # luokitukset (nb! avi loytyy eri suunnasta!)
    httpconn.request('GET', "/koodisto-service/rest/json/relaatio/sisaltyy-ylakoodit/%s" % i["koodiUri"])
    rr = httpconn.getresponse()
    jj = json.loads(rr.read())
    ss = ""
    for ii in jj:
      if ii["koodisto"]["koodistoUri"] == "aluehallintovirasto":
        avikoodi = jv(ii,"koodiArvo")
        avinimi = haenimi(ii,"FI")
        avinimi_sv = haenimi(ii,"SV")
        avinimi_en = haenimi(ii,"EN")
    # muut luokitukset
    httpconn.request('GET', "/koodisto-service/rest/json/relaatio/sisaltyy-alakoodit/%s" % i["koodiUri"])
    rr = httpconn.getresponse()
    jj = json.loads(rr.read())
    ss = ""
    for ii in jj:
      if ii["koodisto"]["koodistoUri"] == "maakunta":
        maakuntakoodi = jv(ii,"koodiArvo")
        maakuntanimi = haenimi(ii,"FI")
        maakuntanimi_sv = haenimi(ii,"SV")
        maakuntanimi_en = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "elykeskus":
        elykoodi = jv(ii,"koodiArvo")
        elynimi = haenimi(ii,"FI")
        elynimi_sv = haenimi(ii,"SV")
        elynimi_en = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "kielisuhde":
        kielisuhdekoodi = jv(ii,"koodiArvo")
        kielisuhdenimi = haenimi(ii,"FI")
        kielisuhdenimi_sv = haenimi(ii,"SV")
        kielisuhdenimi_en = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "seutukunta":
        seutukuntakoodi = jv(ii,"koodiArvo")
        seutukuntanimi = haenimi(ii,"FI")
        seutukuntanimi_sv = haenimi(ii,"SV")
        seutukuntanimi_en = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "laani":
        laanikoodi = jv(ii,"koodiArvo")
        laaninimi = haenimi(ii,"FI")
        laaninimi_sv = haenimi(ii,"SV")
        laaninimi_en = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "kuntaryhma":
        kuntaryhmakoodi = jv(ii,"koodiArvo")
        kuntaryhmanimi = haenimi(ii,"FI")
        kuntaryhmanimi_sv = haenimi(ii,"SV")
        kuntaryhmanimi_en = haenimi(ii,"EN")

    #print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d -- %s" % (lkm,koodi)).encode('utf-8')
    cur.execute("""INSERT INTO SA_ALUELUOKITUS (koodi,nimi,nimi_sv,nimi_en, alkupvm,loppupvm, maakuntakoodi,maakuntanimi,maakuntanimi_sv,maakuntanimi_en, avikoodi,avinimi,avinimi_sv,avinimi_en, elykoodi,elynimi,elynimi_sv,elynimi_en, kielisuhdekoodi,kielisuhdenimi,kielisuhdenimi_sv,kielisuhdenimi_en, seutukuntakoodi,seutukuntanimi,seutukuntanimi_sv,seutukuntanimi_en, laanikoodi,laaninimi,laaninimi_sv,laaninimi_en, kuntaryhmakoodi,kuntaryhmanimi,kuntaryhmanimi_sv,kuntaryhmanimi_en) VALUES (%s,%s,%s,%s, %s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s)""", (koodi,nimi,nimi_sv,nimi_en, alkupvm,loppupvm, maakuntakoodi,maakuntanimi,maakuntanimi_sv,maakuntanimi_en, avikoodi,avinimi,avinimi_sv,avinimi_en, elykoodi,elynimi,elynimi_sv,elynimi_en, kielisuhdekoodi,kielisuhdenimi,kielisuhdenimi_sv,kielisuhdenimi_en, seutukuntakoodi,seutukuntanimi,seutukuntanimi_sv,seutukuntanimi_en, laanikoodi,laaninimi,laaninimi_sv,laaninimi_en, kuntaryhmakoodi,kuntaryhmanimi,kuntaryhmanimi_sv,kuntaryhmanimi_en))
    conn.commit()

  cur.close()
  conn.close()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
    main()
