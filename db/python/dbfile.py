#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
dbfile

From a file given as argument executes SQL file contents to database
which is pointed out via environment variables.

This script depends on script dboperator.
"""
import sys,getopt,os
import pymssql
from time import localtime, strftime
import glob # tiedostojen listaamiseen, kelpaa myös ns wildcardsit

import dboperator

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

def load(sqlfile,migrate,verbose=False):
  show("begin "+sqlfile)

  number_togo = None
  if "__" in sqlfile:
    number_togo = int(sqlfile[sqlfile.rfind("/")+1:sqlfile.index("__",sqlfile.rfind("/")+1)])

  number_last = None
  if number_togo is not None and migrate:
    result = dboperator.get("select max(number) as number from migration where phase='%s'"%(migrate))
    if result[0]["number"] is not None:
      number_last = int(result[0]["number"])
    if verbose: show("migrating %s which is going on %s and now trying %s"%(migrate,number_last,number_togo))

    if number_last is None or number_togo > number_last:
      show("migrating")

      fd = open(sqlfile, 'r')
      sql = fd.read()
      fd.close()
      if verbose: show(sql)
      dboperator.execute(sql)

      result = dboperator.execute("insert into migration (phase,number) values ('%s',%s)"%(migrate,number_togo))
    else:
      if verbose: show("skipping migration %s < %s"%(number_togo,number_last))

  dboperator.close()
  show("ready")

def usage():
  print """
usage: dbfile.py -f|--file <file> [-m|--migrate <phase>] [-v|--verbose]
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  sqlfile = ""
  migrate = ""
  verbose = False

  try:
    opts, args = getopt.getopt(argv,"f:m:v",["file=","migrate=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-f", "--file"): sqlfile = arg
    elif opt in ("-m", "--migrate"): migrate = arg
    elif opt in ("-v", "--verbose"): verbose = True
  if not sqlfile:
    usage()
    sys.exit(2)

  load(sqlfile,migrate,verbose)

if __name__ == "__main__":
  main(sys.argv[1:])
