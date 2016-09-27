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
    avoinKK = getattr(i, "avoinKK", None)
    db = getattr(i, "db", None) #string
    erikoistumisopinnot = getattr(i, "erikoistumisopinnot", None)
    erillinenOO = getattr(i, "erillinenOO", None)
    hyvaksiluetut = getattr(i, "hyvaksiluetut", None)
    joo = getattr(i, "joo", None)
    koodi = getattr(i, "koodi", None) #string
    koulutustyyppi = getattr(i, "koulutustyyppi", None) #string
    kuvaus = getattr(i, "kuvaus", None) #string
    kvVaihto = getattr(i, "kvVaihto", None)
    #luoja #string
    #luontipaivamaara #timestamp
    mValKo = getattr(i, "mValKo", None)
    #paivittaja #string
    #paivityspaivamaara #timestamp
    perustutkinto = getattr(i, "perustutkinto", None)
    tkiHarjoittelunLaajuus = getattr(i, "tkiHarjoittelunLaajuus", None)
    tkiMuutLaajuus = getattr(i, "tkiMuutLaajuus", None)
    tkiToiminnanLaajuus = getattr(i, "tkiToiminnanLaajuus", None)
    vieraskielinen = getattr(i, "vieraskielinen", None)
    vuosi = getattr(i, "vuosi", None)
    
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
