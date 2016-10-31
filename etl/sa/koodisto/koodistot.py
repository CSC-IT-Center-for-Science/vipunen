#!/usr/bin/python

import sys, os
import httplib
import pymssql
import json
from time import localtime, strftime

opintopolkuuri = "testi.virkailija.opintopolku.fi"
httpconn = httplib.HTTPSConnection(opintopolkuuri)

dbhost = os.getenv("PYMSSQL_TEST_SERVER")
dbname = os.getenv("PYMSSQL_TEST_DATABASE")
dbuser = os.getenv("PYMSSQL_TEST_USERNAME")
dbpass = os.getenv("PYMSSQL_TEST_PASSWORD")

def haenimi(i,kieli):
    for m in i["metadata"]:
        if m["kieli"] == kieli:
            return m["nimi"]
    return None

def main():
    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

    #print ("Connecting to database %s" % (dbname)).encode('utf-8')
    conn = pymssql.connect(dbhost, dbuser, dbpass, dbname)
    cur = conn.cursor()

    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan sa_koodistot").encode('utf-8')
    cur.execute("TRUNCATE TABLE sa_koodistot")
    conn.commit()

    koodistouri = "/koodisto-service/rest/json/%s/koodi"
    koodistot = [
        "sukupuoli"
        # alue
        ,"kunta"
        ,"maakunta"
        ,"aluehallintovirasto"
        ,"elykeskus"
        ,"laani"
        ,"seutukunta"
        ,"kuntaryhma"
        ,"kielisuhde"
        # koulutus
        ,"koulutus"
        ,"koulutusasteoph2002"
        ,"koulutusalaoph2002"
        ,"opintoalaoph2002"
        ,"koulutusasteoph1995"
        ,"koulutusalaoph1995"
        ,"opintoalaoph1995"
        ,"tutkinto"
        ,"tutkintotyyppi"
        ,"koulutustyyppi"
        ,"isced2011koulutusaste"
        ,"isced2011koulutusastetaso1"
        ,"isced2011koulutusastetaso2"
        ,"isced2011koulutusalataso1"
        ,"isced2011koulutusalataso2"
        ,"isced2011koulutusalataso3"
        ,"okmohjauksenala"
        # oppilaitos
        ,"oppilaitosnumero"
        ,"oppilaitostyyppi"
        ,"koulutustoimija"
        # vipunen
        ,"vipunenkorkeakouluoppilaitosmap"
    ]
    for koodisto in koodistot:
        url = koodistouri % koodisto
        print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" haetaan opintopolusta -- "+url).encode('utf-8')
        httpconn.request('GET', url)
        r = httpconn.getresponse()
        j = json.loads(r.read())
        lkm = 0
        for i in j:
            lkm += 1
            # sarakkeet
            koodi = i["koodiArvo"]
            nimi = haenimi(i,"FI")
            nimi_sv = haenimi(i,"SV")
            nimi_en = haenimi(i,"EN")
            alkupvm = i["voimassaAlkuPvm"]
            loppupvm = i["voimassaLoppuPvm"]

            #print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %s -- %d -- %s" % (koodisto,lkm,koodi)).encode('utf-8')
            cur.execute("""
            INSERT INTO sa_koodistot (koodisto,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm ,source)
            VALUES (%s,%s,%s,%s,%s,%s,%s ,%s)""", (koodisto,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm ,opintopolkuuri+url))
            conn.commit()

    cur.close()
    conn.close()

    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
    main()
