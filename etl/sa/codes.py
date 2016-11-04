#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
codes

todo doc
"""
import sys, getopt
import httplib
import json
from time import localtime, strftime

import dboperator

def haenimi(i,kieli):
  for m in i["metadata"]:
    if m["kieli"] == kieli:
      return m["nimi"]
  return None

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

def load(secure,hostname,url,table,codeset,verbose=False):
  if verbose: show("begin")

  # tehdään "columnlist" erikseen itse (type ei merkitystä, ei tehdä taulua vaan se on jo)
  row = {'koodisto':None,'koodi':None,'nimi':None,'nimi_sv':None,'nimi_en':None,'alkupvm':None,'loppupvm':None}
  # tämä kutsu alustaa dboperatorin muuttujat, jotta insert-kutsu toimii
  dboperator.columns(row)

  #show("empty sa_koodistot")
  #dboperator.empty("sa_koodistot")

  url = url % codeset
  address = hostname+url
  if secure:
    show("load securely from "+address)
    httpconn = httplib.HTTPSConnection(hostname)
  else:
    show("load from "+address)
    httpconn = httplib.HTTPConnection(hostname)

  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  cnt = 0
  if verbose: show("delete from %s where koodisto=%s"%(table,codeset))
  dboperator.remove(table,"koodisto",codeset)
  for i in j:
    cnt += 1
    # tee "row"
    row["koodisto"] = codeset
    # sarakkeet
    row["koodi"] = i["koodiArvo"]
    row["nimi"] = haenimi(i,"FI")
    row["nimi_sv"] = haenimi(i,"SV")
    row["nimi_en"] = haenimi(i,"EN")
    row["alkupvm"] = i["voimassaAlkuPvm"]
    row["loppupvm"] = i["voimassaLoppuPvm"]

    if verbose: show("-- %s -- %d -- %s"%(codeset,cnt,row["koodi"]))
    dboperator.insert(address,table,row)

  dboperator.close()

  if verbose: show("ready")

def usage():
  print """
usage: codes.py [-s|--secure] [-H|--hostname <hostname>] [-u|--url <url>] [-t|--table <table>] -c|--codeset <codeset> [-v|--verbose]

secure defaults to being secure (HTTPS) (so no point in using this argument at all)
hostname defaults to "testi.virkailija.opintopolku.fi"
url defaults to "/koodisto-service/rest/json/%s/koodi" (do notice the %s in middle which is a placeholder for codeset argument)
table defaults to "sa_koodistot"
codeset is the only mandatory argument. No default. Name of the "koodisto" to be loaded.
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  secure = True # tässä tapauksessa oletetaan secure!
  hostname = "testi.virkailija.opintopolku.fi" # hostname oletuksella
  url = "/koodisto-service/rest/json/%s/koodi" # url oletuksella (nb %s)
  table = "sa_koodistot" # table oletuksella
  codeset = ""
  verbose = False

  try:
    opts, args = getopt.getopt(argv,"sH:u:t:c:vd",["secure","hostname=","url=","table=","codeset=","verbose"])
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
  if not hostname or not url or not table or not codeset:
    usage()
    sys.exit(2)

  load(secure,hostname,url,table,codeset,verbose)

if __name__ == "__main__":
  main(sys.argv[1:])
