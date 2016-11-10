#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
alueluokitus

todo doc
"""
import sys, getopt
import httplib
import json
from time import localtime, strftime

import dboperator

def teerow():
  return {
    'koodi':None, 'nimi':None, 'nimi_sv':None, 'nimi_en':None, 'alkupvm':None, 'loppupvm':None,
    'maakuntakoodi':None, 'maakuntanimi':None, 'maakuntanimi_sv':None, 'maakuntanimi_en':None,
    'avikoodi':None, 'avinimi':None, 'avinimi_sv':None, 'avinimi_en':None,
    'elykoodi':None, 'elynimi':None, 'elynimi_sv':None, 'elynimi_en':None,
    'kielisuhdekoodi':None, 'kielisuhdenimi':None, 'kielisuhdenimi_sv':None, 'kielisuhdenimi_en':None,
    'seutukuntakoodi':None, 'seutukuntanimi':None, 'seutukuntanimi_sv':None, 'seutukuntanimi_en':None,
    'laanikoodi':None, 'laaninimi':None, 'laaninimi_sv':None, 'laaninimi_en':None,
    'kuntaryhmakoodi':None, 'kuntaryhmanimi':None, 'kuntaryhmanimi_sv':None, 'kuntaryhmanimi_en':None
  }

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

def load(secure,hostname,url,table,codeset,verbose=False,debug=False):
  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"

  # tehdään "columnlist" erikseen itse (type ei merkitystä, ei tehdä taulua vaan se on jo)
  row = teerow()
  # tämä kutsu alustaa dboperatorin muuttujat, jotta insert-kutsu toimii
  dboperator.columns(row,debug)
  
  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" empty %s"%(table)
  dboperator.empty(table,debug)

  url = url % codeset # korvaa placeholder
  if secure:
    httpconn = httplib.HTTPSConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load securely from "+hostname+url
  else:
    httpconn = httplib.HTTPConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url

  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  cnt = 0
  for i in j:
    cnt += 1
    # tee "row" (tyhjätään arvot)
    row = teerow()

    row["koodi"] = jv(i,"koodiArvo")
    row["nimi"] = haenimi(i,"FI")
    row["nimi_sv"] = haenimi(i,"SV")
    row["nimi_en"] = haenimi(i,"FI")
    row["alkupvm"] = jv(i,"voimassaAlkuPvm")
    row["loppupvm"] = jv(i,"voimassaLoppuPvm")

    # luokitukset (nb! avi loytyy eri suunnasta!)
    httpconn.request('GET', "/koodisto-service/rest/json/relaatio/sisaltyy-ylakoodit/%s" % i["koodiUri"])
    rr = httpconn.getresponse()
    jj = json.loads(rr.read())
    ss = ""
    for ii in jj:
      if ii["koodisto"]["koodistoUri"] == "aluehallintovirasto":
        row["avikoodi"] = jv(ii,"koodiArvo")
        row["avinimi"] = haenimi(ii,"FI")
        row["avinimi_sv"] = haenimi(ii,"SV")
        row["avinimi_en"] = haenimi(ii,"EN")
    # muut luokitukset
    httpconn.request('GET', "/koodisto-service/rest/json/relaatio/sisaltyy-alakoodit/%s" % i["koodiUri"])
    rr = httpconn.getresponse()
    jj = json.loads(rr.read())
    ss = ""
    for ii in jj:
      if ii["koodisto"]["koodistoUri"] == "maakunta":
        row["maakuntakoodi"] = jv(ii,"koodiArvo")
        row["maakuntanimi"] = haenimi(ii,"FI")
        row["maakuntanimi_sv"] = haenimi(ii,"SV")
        row["maakuntanimi_en"] = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "elykeskus":
        row["elykoodi"] = jv(ii,"koodiArvo")
        row["elynimi"] = haenimi(ii,"FI")
        row["elynimi_sv"] = haenimi(ii,"SV")
        row["elynimi_en"] = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "kielisuhde":
        row["kielisuhdekoodi"] = jv(ii,"koodiArvo")
        row["kielisuhdenimi"] = haenimi(ii,"FI")
        row["kielisuhdenimi_sv"] = haenimi(ii,"SV")
        row["kielisuhdenimi_en"] = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "seutukunta":
        row["seutukuntakoodi"] = jv(ii,"koodiArvo")
        row["seutukuntanimi"] = haenimi(ii,"FI")
        row["seutukuntanimi_sv"] = haenimi(ii,"SV")
        row["seutukuntanimi_en"] = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "laani":
        row["laanikoodi"] = jv(ii,"koodiArvo")
        row["laaninimi"] = haenimi(ii,"FI")
        row["laaninimi_sv"] = haenimi(ii,"SV")
        row["laaninimi_en"] = haenimi(ii,"EN")
      if ii["koodisto"]["koodistoUri"] == "kuntaryhma":
        row["kuntaryhmakoodi"] = jv(ii,"koodiArvo")
        row["kuntaryhmanimi"] = haenimi(ii,"FI")
        row["kuntaryhmanimi_sv"] = haenimi(ii,"SV")
        row["kuntaryhmanimi_en"] = haenimi(ii,"EN")

    if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %d -- %s"%(cnt,row["koodi"])
    dboperator.insert(hostname+url,table,row,debug)

  dboperator.close(debug)

  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"

def usage():
  print """
usage: alueluokitus.py [-s|--secure] [-H|--hostname <hostname>] [-u|--url <url>] [-t|--table <table>] -c|--codeset <codeset> [-v|--verbose] [-d|--debug]

secure defaults to being secure (HTTPS) (so no point in using this argument at all)
hostname defaults to "testi.virkailija.opintopolku.fi"
url defaults to "/koodisto-service/rest/json/%s/koodi" (do notice the %s in middle which is a placeholder for codeset argument)
table defaults to "sa_alueluokitus"
codeset defaults to "kunta"
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  secure = True # tässä tapauksessa oletetaan secure!
  hostname = "testi.virkailija.opintopolku.fi" # hostname oletuksella
  url = "/koodisto-service/rest/json/%s/koodi" # url oletuksella (nb %s)
  table = "sa_alueluokitus" # table oletuksella
  codeset = "kunta" # nb! alueluokitus jolloin "avain" on kunta
  verbose,debug = False,False
  
  try:
    opts, args = getopt.getopt(argv,"sH:u:t:c:vd",["secure","hostname=","url=","table=","codeset=","verbose","debug"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--secure"): secure = True
    elif opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
    elif opt in ("-c", "--codeset"): codeset = arg
    elif opt in ("-v", "--verbose"): verbose = True
    elif opt in ("-d", "--debug"): debug = True
  if not hostname or not url or not table or not codeset:
    usage()
    sys.exit(2)

  if debug: print "debugging"

  load(secure,hostname,url,table,codeset,verbose,debug)

if __name__ == "__main__":
  main(sys.argv[1:])
