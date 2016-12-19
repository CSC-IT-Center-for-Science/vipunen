#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
julkaisutyyppiluokitus

todo doc
"""
import sys, os, getopt
import httplib
import json
from time import localtime, strftime

import dboperator

def makerow():
  return {
    'koodi':None, 'nimi':None, 'nimi_sv':None, 'nimi_en':None, 'alkupvm':None, 'loppupvm':None,
    'julkaisunpaaluokkakoodi':None, 'julkaisunpaaluokkanimi':None, 'julkaisunpaaluokkanimi_sv':None, 'julkaisunpaaluokkanimi_en':None
  }

# get value from json
def jv(jsondata, key):
  if key in jsondata:
    return jsondata[key]
  return None

def getnimi(i,kieli):
    for m in i["metadata"]:
        if m["kieli"] == kieli:
            return m["nimi"]
    return None

def load(secure,hostname,url,table,codeset,verbose=False,debug=False):
  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"

  row = makerow()
  dboperator.columns(row,debug)

  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" empty %s"%(table)
  dboperator.empty(table,debug)

  url = url % codeset # replace placeholder
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
    row = makerow()

    row["koodi"] = jv(i,"koodiArvo")
    row["nimi"] = getnimi(i,"FI")
    row["nimi_sv"] = getnimi(i,"SV")
    row["nimi_en"] = getnimi(i,"FI")
    row["alkupvm"] = jv(i,"voimassaAlkuPvm")
    row["loppupvm"] = jv(i,"voimassaLoppuPvm")

    httpconn.request('GET', "/koodisto-service/rest/json/relaatio/sisaltyy-ylakoodit/%s" % i["koodiUri"])
    rr = httpconn.getresponse()
    jj = json.loads(rr.read())
    ss = ""
    for ii in jj:
      if ii["koodisto"]["koodistoUri"] == "julkaisunpaaluokka":
        row["julkaisunpaaluokkakoodi"] = jv(ii,"koodiArvo")
        row["julkaisunpaaluokkanimi"] = getnimi(ii,"FI")
        row["julkaisunpaaluokkanimi_sv"] = getnimi(ii,"SV")
        row["julkaisunpaaluokkanimi_en"] = getnimi(ii,"EN")

    if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %d -- %s"%(cnt,row["koodi"])
    dboperator.insert(hostname+url,table,row,debug)

  dboperator.close(debug)

  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"

def usage():
  print """
usage: julkaisutyyppiluokitus.py [-s|--secure] [-H|--hostname <hostname>] [-u|--url <url>] [-t|--table <table>] -c|--codeset <codeset> [-v|--verbose] [-d|--debug]

secure defaults to being secure (HTTPS) (so no point in using this argument at all)
hostname defaults to $OPINTOPOLKU then to "testi.virkailija.opintopolku.fi"
url defaults to "/koodisto-service/rest/json/%s/koodi" (do notice the %s in middle which is a placeholder for codeset argument)
table defaults to "sa_julkaisutyyppiluokitus"
codeset defaults to "julkaisutyyppi"
"""

def main(argv):
  # variables from arguments with possible defaults
  secure = True # default secure, so always secure!
  hostname = os.getenv("OPINTOPOLKU") or "testi.virkailija.opintopolku.fi"
  url = "/koodisto-service/rest/json/%s/koodi" # nb %s
  table = "sa_julkaisutyyppiluokitus"
  codeset = "julkaisutyyppi"
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
