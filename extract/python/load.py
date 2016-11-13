#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
load

todo doc
"""
import sys,os,getopt
import httplib,ssl,base64
import json
from time import localtime, strftime

import dboperator

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

def load(secure,hostname,url,table,postdata,condition,verbose):
  show("begin "+hostname+" "+url+" "+table+" "+postdata+" "+condition)
  address=hostname+url
  if secure:
    show("load securely from "+address)
    httpconn=httplib.HTTPSConnection(hostname, context=ssl._create_unverified_context())
  else:
    show("load from "+address)
    httpconn=httplib.HTTPConnection(hostname)

  # api-yhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
  # nb! post-operaatioon data saadaan python-placeholder-merkkijono.
  # tämä on sarakkeita/tietokenttiä nimetty. Voisiko post-parametriä käyttää?
  if postdata:
    headers=""
    if os.getenv("API_USERNAME"):
      apiuser = os.getenv("API_USERNAME")
      apipass = os.getenv("API_PASSWORD")
      headers={'Content-Type': 'application/json', 'Authorization': 'Basic %s' % base64.b64encode(apiuser+":"+apipass)}
    httpconn.request('POST', url, postdata, headers)
  else:
    httpconn.request('GET', url)

  r=httpconn.getresponse()
  j=json.loads(r.read())
  show("api returned %d objects"%(len(j)))
  # selvitä sarakkeet; luupataan kaikki!
  # (voisi parametroida tai rajoittaa jotenkin...)
  for row in j:
    dboperator.columns(row)

  # tietojen poistaminen ehdolla
  # merge-operaatio voisi tulla kyseeseen, eli katso mitä ei vielä ole kannassa...
  if condition:
    show("remove from %s with condition '%s'"%(table,condition))
    dboperator.execute("DELETE FROM %s WHERE %s"%(table,condition))
  else:
    show("create and/or empty %s"%(table))
    dboperator.create(table)

  show("insert data")
  cnt=0
  for row in j:
    cnt+=1
    if verbose: show("%d -- %s"%(cnt,row))
    dboperator.insert(address,table,row)

  show("wrote %d"%(cnt))
  dboperator.close()

  show("ready")

def usage():
  print """
usage: load.py [-s|--secure] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-p|--postdata] [-c|--condition <condition>] [-v|--verbose]
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  secure=False
  hostname,url,table="","",""
  postdata=""
  condition=""
  verbose=False

  try:
    opts,args=getopt.getopt(argv,"sH:u:t:p:c:v",["secure","hostname=","url=","table=","postdata=","condition=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt,arg in opts:
    if opt in ("-s", "--secure"): secure=True
    elif opt in ("-H", "--hostname"): hostname=arg
    elif opt in ("-u", "--url"): url=arg
    elif opt in ("-t", "--table"): table=arg
    elif opt in ("-p", "--postdata"): postdata=arg
    elif opt in ("-c", "--condition"): condition=arg
    elif opt in ("-v", "--verbose"): verbose=True
  if not hostname or not url or not table:
    usage()
    sys.exit(2)

  load(secure,hostname,url,table,postdata,condition,verbose)

if __name__ == "__main__":
    main(sys.argv[1:])
