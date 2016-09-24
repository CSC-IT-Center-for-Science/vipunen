#!/usr/bin/python

from time import localtime, strftime

import sys, os

import httplib
opintopolkuuri = "virkailija.opintopolku.fi"
httpconn = httplib.HTTPSConnection(opintopolkuuri)

import json

import pymssql
<<<<<<< HEAD
=======
import os
>>>>>>> 386b85f3395ac42f461a4a1ccfe6fc9406025d29
server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

def haenimi(i,kieli):
    for m in i["metadata"]:
        if m["kieli"] == kieli:
            return m["nimi"]
    return None

def main():
    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" alkaa").encode('utf-8')

    print ("Connecting to database\n ->%s" % (database)).encode('utf-8')
    conn = pymssql.connect(server, user, password, database)
    cur = conn.cursor()

    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan SA_KOODISTOT").encode('utf-8')
    cur.execute("DELETE FROM SA_KOODISTOT")
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

            #print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %s -- %d -- (%s,%s,%s,%s,%s,%s)" % (koodisto,lkm,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm)).encode('utf-8')
            print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %s -- %d -- %s" % (koodisto,lkm,koodi)).encode('utf-8')
            cur.execute("""INSERT INTO SA_KOODISTOT (koodisto,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (koodisto,koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm))
            conn.commit()

    cur.close()
    conn.close()

    print (strftime("%Y-%m-%d %H:%M:%S", localtime())+" valmis").encode('utf-8')

if __name__ == "__main__":
    main()
