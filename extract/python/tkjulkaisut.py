#!/usr/bin/python

from time import localtime, strftime
import sys, os

import httplib
sourcehostname = "dwitjutife1.csc.fi"
httpconn = httplib.HTTPSConnection(sourcehostname)
#httpconn = httplib.HTTPConnection(sourcehostname)

import json

import pymssql
server = os.getenv("DATABASE_HOST")
database = os.getenv("DATABASE_NAME")
user = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASS")
# muuten dboperator, mutta koko tämä skripti olisi tarkoitus hoitaa load.py-skriptillä

# hae avaimen arvo json:sta
def jv(jsondata, key):
  if key in jsondata:
    if type(jsondata[key]) is list:
      return ','.join(jsondata[key])
    else:
      return jsondata[key]
  return None

def main():
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

  #print ("Connecting to database %s" % (database)).encode('utf-8')
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan sa_virta_jtp_tkjulkaisut").encode('utf-8')
  cur.execute("DELETE FROM sa_virta_jtp_tkjulkaisut")
  conn.commit()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" haetaan %s" % (sourcehostname)).encode('utf-8')
  apiuri = "/api/TKjulkaisut"
  httpconn.request('GET', apiuri)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  lkm = 0
  for i in j:
    lkm += 1
    # sarakkeet
    organisaatioTunnus = jv(i, "organisaatioTunnus")
    ilmoitusVuosi = jv(i, "ilmoitusVuosi")
    julkaisunTunnus = jv(i, "julkaisunTunnus")
    julkaisunTilakoodi = jv(i, "julkaisunTilakoodi")
    julkaisunOrgTunnus = jv(i, "julkaisunOrgTunnus")
    julkaisuVuosi = jv(i, "julkaisuVuosi")
    julkaisunNimi = jv(i, "julkaisunNimi")
    tekijatiedotTeksti = jv(i, "tekijatiedotTeksti")
    tekijoidenLkm = jv(i, "tekijoidenLkm")
    sivunumeroTeksti = jv(i, "sivunumeroTeksti")
    artikkelinumero = jv(i, "artikkelinumero")
    isbn = jv(i, "isbn")
    jufoTunnus = jv(i, "jufoTunnus")
    jufoLuokkaKoodi = jv(i, "jufoLuokkaKoodi")
    julkaisumaaKoodi = jv(i, "julkaisumaaKoodi")
    lehdenNimi = jv(i, "lehdenNimi")
    issn = jv(i, "issn")
    volyymiTeksti = jv(i, "volyymiTeksti")
    lehdenNumeroTeksti = jv(i, "lehdenNumeroTeksti")
    konferenssinNimi = jv(i, "konferenssinNimi")
    kustantajanNimi = jv(i, "kustantajanNimi")
    kustannuspaikkaTeksti = jv(i, "kustannuspaikkaTeksti")
    emojulkaisunNimi = jv(i, "emojulkaisunNimi")
    emojulkaisunToimittajatTeksti = jv(i, "emojulkaisunToimittajatTeksti")
    julkaisuntyyppiKoodi = jv(i, "julkaisuntyyppiKoodi")
    yhteisjulkaisuKVKytkin = jv(i, "yhteisjulkaisuKVKytkin")
    yhteisjulkaisuSHPKytkin = jv(i, "yhteisjulkaisuSHPKytkin")
    yhteisjulkaisuTutkimuslaitosKytkin = jv(i, "yhteisjulkaisuTutkimuslaitosKytkin")
    yhteisjulkaisuMuuKytkin = jv(i, "yhteisjulkaisuMuuKytkin")
    julkaisunKansainvalisyysKytkin = jv(i, "julkaisunKansainvalisyysKytkin")
    julkaisunKieliKoodi = jv(i, "julkaisunKieliKoodi")
    avoinSaatavuusKoodi = jv(i, "avoinSaatavuusKoodi")
    evoJulkaisunKytkin = jv(i, "evoJulkaisunKytkin")
    doi = jv(i, "doi")
    pysyvaOsoiteTeksti = jv(i, "pysyvaOsoiteTeksti")
    lahdetietokannanTunnus = jv(i, "lahdetietokannanTunnus")
    latausId = jv(i, "latausId")
    yhteisjulkaisuId = jv(i, "yhteisjulkaisuId")
    rinnakkaistallennusKytkin = jv(i, "rinnakkaistallennusKytkin")
    yhteisjulkaisunTunnus = jv(i, "yhteisjulkaisunTunnus")
    juuliOsoiteTeksti = jv(i, "juuliOsoiteTeksti")
    yhteisjulkaisuYritysKytkin = jv(i, "yhteisjulkaisuYritysKytkin")
    jufoId = jv(i, "jufoId")
    hankeTKs = jv(i, "hankeTKs")
    avainsanaTKs = jv(i, "avainsanaTKs")
    isbnTKs = jv(i, "isbnTKs")
    issnTKs = jv(i, "issnTKs")
    koulutusalaTKs = jv(i, "koulutusalaTKs")
    orgYksikkoTKs = jv(i, "orgYksikkoTKs")
    tekijaTKs = jv(i, "tekijaTKs")
    tieteenalaTKs = jv(i, "tieteenalaTKs")

    #if lkm%1000 == 0:
    #  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d" % (lkm)).encode('utf-8')
    cur.execute("""
    INSERT INTO sa_virta_jtp_julkaisut
    (organisaatioTunnus, ilmoitusVuosi, julkaisunTunnus, julkaisunTilakoodi, julkaisunOrgTunnus, julkaisuVuosi,
     julkaisunNimi, tekijatiedotTeksti, tekijoidenLkm, sivunumeroTeksti, artikkelinumero, isbn, jufoTunnus,
     jufoLuokkaKoodi, julkaisumaaKoodi, lehdenNimi, issn, volyymiTeksti, lehdenNumeroTeksti, konferenssinNimi,
     kustantajanNimi, kustannuspaikkaTeksti, emojulkaisunNimi, emojulkaisunToimittajatTeksti, julkaisuntyyppiKoodi,
     yhteisjulkaisuKVKytkin, yhteisjulkaisuSHPKytkin, yhteisjulkaisuTutkimuslaitosKytkin, yhteisjulkaisuMuuKytkin,
     julkaisunKansainvalisyysKytkin, julkaisunKieliKoodi, avoinSaatavuusKoodi, evoJulkaisunKytkin, doi,
     pysyvaOsoiteTeksti, lahdetietokannanTunnus, latausId, yhteisjulkaisuId, rinnakkaistallennusKytkin,
     yhteisjulkaisunTunnus, juuliOsoiteTeksti, yhteisjulkaisuYritysKytkin, jufoId,
     hankeTKs, avainsanaTKs, isbnTKs, issnTKs, koulutusalaTKs, orgYksikkoTKs, tekijaTKs, tieteenalaTKs)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s)
    """, (organisaatioTunnus, ilmoitusVuosi, julkaisunTunnus, julkaisunTilakoodi, julkaisunOrgTunnus, julkaisuVuosi,
     julkaisunNimi, tekijatiedotTeksti, tekijoidenLkm, sivunumeroTeksti, artikkelinumero, isbn, jufoTunnus,
     jufoLuokkaKoodi, julkaisumaaKoodi, lehdenNimi, issn, volyymiTeksti, lehdenNumeroTeksti, konferenssinNimi,
     kustantajanNimi, kustannuspaikkaTeksti, emojulkaisunNimi, emojulkaisunToimittajatTeksti, julkaisuntyyppiKoodi,
     yhteisjulkaisuKVKytkin, yhteisjulkaisuSHPKytkin, yhteisjulkaisuTutkimuslaitosKytkin, yhteisjulkaisuMuuKytkin,
     julkaisunKansainvalisyysKytkin, julkaisunKieliKoodi, avoinSaatavuusKoodi, evoJulkaisunKytkin, doi,
     pysyvaOsoiteTeksti, lahdetietokannanTunnus, latausId, yhteisjulkaisuId, rinnakkaistallennusKytkin,
     yhteisjulkaisunTunnus, juuliOsoiteTeksti, yhteisjulkaisuYritysKytkin, jufoId,
     hankeTKs, avainsanaTKs, isbnTKs, issnTKs, koulutusalaTKs, orgYksikkoTKs, tekijaTKs, tieteenalaTKs))
    conn.commit()

  cur.close()
  conn.close()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
  main()
