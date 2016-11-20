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

def load(sqlfile,verbose=False):
  show("begin "+sqlfile)

  fd = open(sqlfile, 'r')
  sql = fd.read()
  fd.close()
  if verbose: show(sql)
  dboperator.execute(sql)
  
  dboperator.close()    
  show("ready")

def usage():
  print """
usage: dbfile.py [-f|--file <file>] [-v|--verbose]
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  sqlfile = ""
  verbose = False
  
  try:
    opts, args = getopt.getopt(argv,"f:v",["file=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-f", "--file"): sqlfile = arg
    elif opt in ("-v", "--verbose"): verbose = True
  if not sqlfile:
    usage()
    sys.exit(2)

  load(sqlfile,verbose)
    
if __name__ == "__main__":
  main(sys.argv[1:])
