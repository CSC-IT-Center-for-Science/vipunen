#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,getopt
import httplib
import json
from time import localtime, strftime

import dboperator

def load(secure,hostname,url,table,verbose=False,debug=False):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"

  if secure:
    httpconn = httplib.HTTPSConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load securely from "+hostname+url
  else:
    httpconn = httplib.HTTPConnection(hostname)
    print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url

  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" got %d" % (len(j))
  # selvitä sarakkeet; luupataan kaikki! (voisi parametroida...)
  for row in j:
    dboperator.columns(row,debug)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" create and/or truncate %s" % (table)
  dboperator.create(table,debug)
  cnt = 0
  for row in j:
    cnt += 1
    if verbose: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %d" % (cnt)
    dboperator.insert(hostname+url,table,row,debug)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" wrote %d" % (cnt)
  dboperator.close(debug)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"

def usage():
  print 'usage: sa.py [-s|--secure] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-v|--verbose] [-d|--debug]'

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  secure = False
  hostname,url,table = "","",""
  verbose,debug = False,False

  try:
    opts, args = getopt.getopt(argv,"sH:u:t:vd",["secure","hostname=","url=","table=","verbose","debug"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--secure"): secure = True
    elif opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
    elif opt in ("-v", "--verbose"): verbose = True
    elif opt in ("-d", "--debug"): debug = True
  if not hostname or not url or not table:
    usage()
    sys.exit(2)

  if debug: print "debugging"

  load(secure,hostname,url,table,verbose,debug)

if __name__ == "__main__":
    main(sys.argv[1:])
