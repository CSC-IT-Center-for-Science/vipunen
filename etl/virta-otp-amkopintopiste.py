#!/usr/bin/python

from time import localtime, strftime

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
        avoinKK = i["avoinKK"]
        db = i["db"] #string
        erikoistumisopinnot = i["erikoistumisopinnot"]
        erillinenOO = i["erillinenOO"]
        hyvaksiluetut = i["hyvaksiluetut"]
        joo = i["joo"]
        koodi = i["koodi"] #string
        koulutustyyppi = i["koulutustyyppi"] #string
        kuvaus = i["kuvaus"] #string
        kvVaihto = i["kvVaihto"]
        #luoja #string
        #luontipaivamaara #timestamp
        mValKo = i["mValKo"]
        #paivittaja #string
        #paivityspaivamaara #timestamp
        perustutkinto = i["perustutkinto"]
        tkiHarjoittelunLaajuus = i["tkiHarjoittelunLaajuus"]
        tkiMuutLaajuus = i["tkiMuutLaajuus"]
        tkiToiminnanLaajuus = i["tkiToiminnanLaajuus"]
        vieraskielinen = i["vieraskielinen"]
        vuosi = i["vuosi"]
        
        print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %d -- %s %d %s" % (lkm,db,vuosi,koodi)).encode('utf-8')
        cur.execute("""INSERT INTO SA_VIRTA_OTP_AMKOPINTOPISTE (avoinKK, db, erikoistumisopinnot, erillinenOO, hyvaksiluetut, joo, koodi, koulutustyyppi, kuvaus, kvVaihto, mValKo, perustutkinto, tkiHarjoittelunLaajuus, tkiMuutLaajuus, tkiToiminnanLaajuus, vieraskielinen, vuosi) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s)""", (avoinKK, db, erikoistumisopinnot, erillinenOO, hyvaksiluetut, joo, koodi, koulutustyyppi, kuvaus, kvVaihto, mValKo, perustutkinto, tkiHarjoittelunLaajuus, tkiMuutLaajuus, tkiToiminnanLaajuus, vieraskielinen, vuosi))
        conn.commit()

    cur.close()
    conn.close()

    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
    main()
