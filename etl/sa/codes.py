#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
codes

doc todo
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

def load(secure,hostname,url,table,codeset,verbose=False,debug=False):
  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"
  
  # tehdään "columnlist" erikseen itse (type ei merkitystä)
  row = {'koodisto':None,'koodi':None,'nimi':None,'nimi_sv':None,'nimi_en':None,'alkupvm':None,'loppupvm':None}
  # tämä kutsu alustaa dboperatorin muuttujat, jotta insert-kutsu toimii
  dboperator.columns(row,debug)
  
  #print strftime("%Y-%m-%d %H:%M:%S", localtime())+" tyhjennetaan sa_koodistot"
  #dboperator.empty("sa_koodistot")

  url = url % codeset
  if secure:
    httpconn = httplib.HTTPSConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load securely from "+hostname+url
  else:
    httpconn = httplib.HTTPConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url

  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  lkm = 0
  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" delete from %s where koodisto=%s"%(table,codeset)
  dboperator.remove(table,"koodisto",codeset)
  for i in j:
    lkm += 1
    # tee "row"
    row["koodisto"] = codeset
    # sarakkeet
    row["koodi"] = i["koodiArvo"]
    row["nimi"] = haenimi(i,"FI")
    row["nimi_sv"] = haenimi(i,"SV")
    row["nimi_en"] = haenimi(i,"EN")
    row["alkupvm"] = i["voimassaAlkuPvm"]
    row["loppupvm"] = i["voimassaLoppuPvm"]

    if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" -- %s -- %d -- %s"%(codeset,lkm,row["koodi"])
    dboperator.insert(hostname+url,table,row,debug)

  dboperator.close()

  if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"

def usage():
  print """
usage: koodistot.py [-s|--secure] [-H|--hostname <hostname>] [-u|--url <url>] [-t|--table <table>] -c|--codeset <codeset> [-v|--verbose] [-d|--debug]

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
