#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
tkjulkaisut

todo doc
"""
import sys,os,getopt
import httplib,ssl,base64
import json
from time import localtime, strftime

import dboperator

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

# hae avaimen arvo json:sta
def jv(jsondata, key):
  #print key
  if key in jsondata:
    return jsondata[key]
  return None

def load(hostname,url,table,verbose):
  show("begin "+hostname+" "+url+" "+table)
  address=hostname+url

  show("load securely from "+address)
  httpconn=httplib.HTTPSConnection(hostname, context=ssl._create_unverified_context())

  httpconn.request('GET', url)

  r = httpconn.getresponse()
  j = json.loads(r.read())

  show("api returned %d objects"%(len(j)))

  show("empty %s"%(table))
  dboperator.empty(table)

  show("insert data")
  cnt=0
  for row in j:
    cnt+=1
    if verbose: show("%d -- %s"%(cnt,row))
    else:
      # show some sign of being alive
      if cnt%100 == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
      if cnt%1000 == 0:
        show("-- %d" % (cnt))

    # sarakkeet
    organisaatioTunnus = jv(row, "organisaatioTunnus")
    ilmoitusVuosi = jv(row, "ilmoitusVuosi")
    julkaisunTunnus = jv(row, "julkaisunTunnus")
    julkaisunTilakoodi = jv(row, "julkaisunTilakoodi")
    julkaisunOrgTunnus = jv(row, "julkaisunOrgTunnus")
    julkaisuVuosi = jv(row, "julkaisuVuosi")
    julkaisunNimi = jv(row, "julkaisunNimi")
    tekijatiedotTeksti = jv(row, "tekijatiedotTeksti")
    tekijoidenLkm = jv(row, "tekijoidenLkm")
    sivunumeroTeksti = jv(row, "sivunumeroTeksti")
    artikkelinumero = jv(row, "artikkelinumero")
    isbn = jv(row, "isbn")
    jufoTunnus = jv(row, "jufoTunnus")
    jufoLuokkaKoodi = jv(row, "jufoLuokkaKoodi")
    julkaisumaaKoodi = jv(row, "julkaisumaaKoodi")
    lehdenNimi = jv(row, "lehdenNimi")
    issn = jv(row, "issn")
    volyymiTeksti = jv(row, "volyymiTeksti")
    lehdenNumeroTeksti = jv(row, "lehdenNumeroTeksti")
    konferenssinNimi = jv(row, "konferenssinNimi")
    kustantajanNimi = jv(row, "kustantajanNimi")
    kustannuspaikkaTeksti = jv(row, "kustannuspaikkaTeksti")
    emojulkaisunNimi = jv(row, "emojulkaisunNimi")
    emojulkaisunToimittajatTeksti = jv(row, "emojulkaisunToimittajatTeksti")
    julkaisuntyyppiKoodi = jv(row, "julkaisuntyyppiKoodi")
    yhteisjulkaisuKVKytkin = jv(row, "yhteisjulkaisuKVKytkin")
    yhteisjulkaisuSHPKytkin = jv(row, "yhteisjulkaisuSHPKytkin")
    yhteisjulkaisuTutkimuslaitosKytkin = jv(row, "yhteisjulkaisuTutkimuslaitosKytkin")
    yhteisjulkaisuMuuKytkin = jv(row, "yhteisjulkaisuMuuKytkin")
    julkaisunKansainvalisyysKytkin = jv(row, "julkaisunKansainvalisyysKytkin")
    julkaisunKieliKoodi = jv(row, "julkaisunKieliKoodi")
    avoinSaatavuusKoodi = jv(row, "avoinSaatavuusKoodi")
    evoJulkaisunKytkin = jv(row, "evoJulkaisunKytkin")
    doi = jv(row, "doi")
    pysyvaOsoiteTeksti = jv(row, "pysyvaOsoiteTeksti")
    lahdetietokannanTunnus = jv(row, "lahdetietokannanTunnus")
    latausId = jv(row, "latausId")
    yhteisjulkaisuId = jv(row, "yhteisjulkaisuId")
    rinnakkaistallennusKytkin = jv(row, "rinnakkaistallennusKytkin")
    yhteisjulkaisunTunnus = jv(row, "yhteisjulkaisunTunnus")
    juuliOsoiteTeksti = jv(row, "juuliOsoiteTeksti")
    yhteisjulkaisuYritysKytkin = jv(row, "yhteisjulkaisuYritysKytkin")
    jufoId = jv(row, "jufoId")

    hankeTKs = jv(row, "hankeTKs")
    hankeTKstr = None
    if type(hankeTKs) is list and len(hankeTKs)>0:
      hankeTKstr = ','.join(str(d) for d in hankeTKs)

    avainsanaTKs = jv(row, "avainsanaTKs")
    avainsanaTKstr = None
    if type(avainsanaTKs) is list and len(avainsanaTKs)>0:
      avainsanaTKstr = ','.join(str(d) for d in avainsanaTKs)

    isbnTKs = jv(row, "isbnTKs")
    isbnTKstr = None
    if type(isbnTKs) is list and len(isbnTKs)>0:
      isbnTKstr = isbnTKs[0]["isbn"]

    issnTKs = jv(row, "issnTKs")
    issnTKstr = None
    if type(issnTKs) is list and len(issnTKs)>0:
      issnTKstr = issnTKs[0]["issn"]

    koulutusalaTKs = jv(row, "koulutusalaTKs")
    koulutusalaTKstr = None
    if type(koulutusalaTKs) is list and len(koulutusalaTKs)>0:
      for d in koulutusalaTKs:
        if d["jNro"] and d["jNro"]==1:
          koulutusalaTKstr = d["koulutusala"]

    orgYksikkoTKs = jv(row, "orgYksikkoTKs")
    orgYksikkoTKstr = None
    if type(orgYksikkoTKs) is list and len(orgYksikkoTKs)>0:
      orgYksikkoTKstr = orgYksikkoTKs[0]["julkaisuYksikko"]

    tekijaTKs = jv(row, "tekijaTKs")
    tekijaTKstr = ""
    if type(tekijaTKs) is list and len(tekijaTKs)>0:
      di=0
      for d in tekijaTKs:
        di=di+1
        if di>1:
          tekijaTKstr = tekijaTKstr+"; "
        if d["sukunimi"]:
          tekijaTKstr = tekijaTKstr+d["sukunimi"]
        if d["etunimet"]:
          tekijaTKstr = tekijaTKstr+", "+d["etunimet"].strip()

    tieteenalaTKs = jv(row, "tieteenalaTKs")
    tieteenalaTKstr = None
    if type(tieteenalaTKs) is list and len(tieteenalaTKs)>0:
      for d in tieteenalaTKs:
        if d["jNro"] and d["jNro"]==1:
          tieteenalaTKstr = d["tieteenala"]

    # find out which columns to use on insert
    ##dboperator.resetcolumns(row)
    ##
    ##for col in row:
    ##  if type(row[col]) is list:
    ##    row[col] = ','.join(str(d) for d in row[col])
    ##dboperator.insert(address,table,row)
    dboperator.execute("""
    INSERT INTO sa_virta_jtp_tkjulkaisut
    (
     organisaatioTunnus, ilmoitusVuosi, julkaisunTunnus, julkaisunTilakoodi, julkaisunOrgTunnus, julkaisuVuosi,
     julkaisunNimi, tekijatiedotTeksti, tekijoidenLkm, sivunumeroTeksti, artikkelinumero, isbn, jufoTunnus,
     jufoLuokkaKoodi, julkaisumaaKoodi, lehdenNimi, issn, volyymiTeksti, lehdenNumeroTeksti, konferenssinNimi,
     kustantajanNimi, kustannuspaikkaTeksti, emojulkaisunNimi, emojulkaisunToimittajatTeksti, julkaisuntyyppiKoodi,
     yhteisjulkaisuKVKytkin, yhteisjulkaisuSHPKytkin, yhteisjulkaisuTutkimuslaitosKytkin, yhteisjulkaisuMuuKytkin,
     julkaisunKansainvalisyysKytkin, julkaisunKieliKoodi, avoinSaatavuusKoodi, evoJulkaisunKytkin, doi,
     pysyvaOsoiteTeksti, lahdetietokannanTunnus, latausId, yhteisjulkaisuId, rinnakkaistallennusKytkin,
     yhteisjulkaisunTunnus, juuliOsoiteTeksti, yhteisjulkaisuYritysKytkin, jufoId,
     hankeTKs, avainsanaTKs, isbnTKs, issnTKs, koulutusalaTKs, orgYksikkoTKs, tekijaTKs, tieteenalaTKs
     ,source
    )
    VALUES (
     %s,%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s,%s,%s
     ,%s
    )
    """, (
     organisaatioTunnus, ilmoitusVuosi, julkaisunTunnus, julkaisunTilakoodi, julkaisunOrgTunnus, julkaisuVuosi,
     julkaisunNimi, tekijatiedotTeksti, tekijoidenLkm, sivunumeroTeksti, artikkelinumero, isbn, jufoTunnus,
     jufoLuokkaKoodi, julkaisumaaKoodi, lehdenNimi, issn, volyymiTeksti, lehdenNumeroTeksti, konferenssinNimi,
     kustantajanNimi, kustannuspaikkaTeksti, emojulkaisunNimi, emojulkaisunToimittajatTeksti, julkaisuntyyppiKoodi,
     yhteisjulkaisuKVKytkin, yhteisjulkaisuSHPKytkin, yhteisjulkaisuTutkimuslaitosKytkin, yhteisjulkaisuMuuKytkin,
     julkaisunKansainvalisyysKytkin, julkaisunKieliKoodi, avoinSaatavuusKoodi, evoJulkaisunKytkin, doi,
     pysyvaOsoiteTeksti, lahdetietokannanTunnus, latausId, yhteisjulkaisuId, rinnakkaistallennusKytkin,
     yhteisjulkaisunTunnus, juuliOsoiteTeksti, yhteisjulkaisuYritysKytkin, jufoId,
     hankeTKstr, avainsanaTKstr, isbnTKstr, issnTKstr, koulutusalaTKstr, orgYksikkoTKstr, tekijaTKstr, tieteenalaTKstr
     ,address
    ))

  show("wrote %d"%(cnt))
  dboperator.close()

  show("ready")

def usage():
  print """
usage: tkjulkaisut.py -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-v|--verbose]
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  hostname,url,table="","",""
  verbose=False

  try:
    opts,args=getopt.getopt(argv,"H:u:t:v",["hostname=","url=","table=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt,arg in opts:
    if opt in ("-H", "--hostname"): hostname=arg
    elif opt in ("-u", "--url"): url=arg
    elif opt in ("-t", "--table"): table=arg
    elif opt in ("-v", "--verbose"): verbose=True
  if not hostname or not url or not table:
    usage()
    sys.exit(2)

  load(hostname,url,table,verbose)

if __name__ == "__main__":
    main(sys.argv[1:])
