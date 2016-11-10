#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
dbfile

From path given as argument with possibility to give wildcards executes
SQL files in naturally sorted order (1,2,3,...,10,11,...) to database
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

def load(wildpath,verbose=False):
  show("begin "+wildpath)

  sqlfiles = glob.glob(wildpath)
  # sortataan tiedoston nimen määrätyn osuuden mukaan:
  sortedsqlfiles = sorted(sqlfiles, key=lambda x: int(x.split('/')[x.count("/")].split('__')[0]))
  cnt = 0
  for sqlfile in sortedsqlfiles:
    cnt += 1
    show("%s -- %s"%(cnt,sqlfile))
    fd = open(sqlfile, 'r')
    sql = fd.read()
    fd.close()
    if verbose: show(sql)
    dboperator.execute(sql)
      
  show("ready")

def usage():
  print """
usage: dbfile.py [-w|--wildpath] [-v|--verbose]

wildpath defaults to "vipunen/db/sa/*.sql" (repository). A wildcarded path for SQL files to be executed.
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  wildpath = "vipunen/db/sa/*.sql" # tiedostojen etsivä wildcard-polku oletuksella
  verbose = False
  
  try:
    opts, args = getopt.getopt(argv,"w:v",["wildpath=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-w", "--wildpath"): wildpath = arg
    elif opt in ("-v", "--verbose"): verbose = True
  if not wildpath:
    usage()
    sys.exit(2)

  load(wildpath,verbose)
    
if __name__ == "__main__":
  main(sys.argv[1:])
