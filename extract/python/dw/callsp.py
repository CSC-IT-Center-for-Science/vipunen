#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
callsp

Vipunen DW ETL

Call a stored procedure in database.
"""
import sys,getopt
from time import localtime, strftime

import dboperator

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

def load(schema,procedure,verbose=False):
  show("begin with "+schema+" "+procedure)

  # selvitä sarakkeet; luupataan kaikki! (voisi parametroida...)
  #dboperator.columns(row)
  # sarakelista koodistosta
  sql = "execute "+schema+"."+procedure
  try:
    dboperator.execute(sql)
  except:
    # lopetus vai ?
    #"""
    show("Something went wrong. Over and out.")
    dboperator.close()
    exit(2) # lopeta virheeseen?
    #"""

  dboperator.close()

  show("ready")

def usage():
  print """
usage: dimension.py [-s|--schema <schema>] -p|--procedure <procedure> [-v|--verbose]

schema defaults to dbo
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  schema = "dbo"
  procedure = ""
  verbose = False

  try:
    opts, args = getopt.getopt(argv,"s:p:v",["schema=","procedure=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--schematable"): schema = arg
    elif opt in ("-p", "--procedure"): procedure = arg
    elif opt in ("-v", "--verbose"): verbose = True
  if not procedure:
    usage()
    sys.exit(2)

  load(schema,procedure,verbose)

if __name__ == "__main__":
    main(sys.argv[1:])
