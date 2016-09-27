#!/usr/bin/python

from time import localtime, strftime
import time

import sys, os

import httplib
sourcehostname = "dwidvirtaws.csc.fi"
#httpconn = httplib.HTTPSConnection(sourcehostname)
httpconn = httplib.HTTPConnection(sourcehostname)

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
  pyalku = time.time()
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

  print ("Connecting to database\n ->%s" % (database)).encode('utf-8')
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan SA_VIRTA_OTP_AMKOPINTOPISTE").encode('utf-8')
  cur.execute("DELETE FROM SA_VIRTA_OTP_AMKOPINTOPISTE")
  conn.commit()

  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" haetaan opintopolusta").encode('utf-8')
  apiuri = "/otp/api/AMKOpintopiste"
  httpconn.request('GET', apiuri)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  lkm = 0
  for i in j:
    lkm += 1
    # sarakkeet
    avoinKK = jv(i, 'avoinKK')
    db = jv(i, "db") #string
    erikoistumisopinnot = jv(i, "erikoistumisopinnot")
    erillinenOO = jv(i, "erillinenOO")
    hyvaksiluetut = jv(i, "hyvaksiluetut")
    joo = jv(i, "joo")
    koodi = jv(i, "koodi") #string
    koulutustyyppi = jv(i, "koulutustyyppi") #string
    kuvaus = jv(i, "kuvaus") #string
    kvVaihto = jv(i, "kvVaihto")
    #luoja #string
    #luontipaivamaara #timestamp
    mValKo = jv(i, "mValKo")
    #paivittaja #string
    #paivityspaivamaara #timestamp
    perustutkinto = jv(i, "perustutkinto")
    tkiHarjoittelunLaajuus = jv(i, "tkiHarjoittelunLaajuus")
    tkiMuutLaajuus = jv(i, "tkiMuutLaajuus")
    tkiToiminnanLaajuus = jv(i, "tkiToiminnanLaajuus")
    vieraskielinen = jv(i, "vieraskielinen")
    vuosi = jv(i, "vuosi")
    
    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d" % (lkm)).encode('utf-8')
    cur.execute("""INSERT INTO SA_VIRTA_OTP_AMKOPINTOPISTE (avoinKK, db, erikoistumisopinnot, erillinenOO, hyvaksiluetut, joo, koodi, koulutustyyppi, kuvaus, kvVaihto, mValKo, perustutkinto, tkiHarjoittelunLaajuus, tkiMuutLaajuus, tkiToiminnanLaajuus, vieraskielinen, vuosi) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s)""", (avoinKK, db, erikoistumisopinnot, erillinenOO, hyvaksiluetut, joo, koodi, koulutustyyppi, kuvaus, kvVaihto, mValKo, perustutkinto, tkiHarjoittelunLaajuus, tkiMuutLaajuus, tkiToiminnanLaajuus, vieraskielinen, vuosi))
    conn.commit()

  cur.close()
  conn.close()

  pyloppu = time.time()
  pykesto = pyloppu - pyalku
  print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')
  print ("ajo kesti %f"%(pykesto)).encode('utf-8')

if __name__ == "__main__":
  main()
