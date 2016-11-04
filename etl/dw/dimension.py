#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
"""
dimension

Vipunen DW ETL

Load from database VIPUNEN_SA and table dbo.sa_koodistot with column koodisto
value equal to argument table and create to database which is given in
environment variable PYMSSQL_TEST_DATABASE to a schema possibly given in
argument (default dbo) to table given in argument (mandatory and same as that
value in koodisto) with prefix "d_" (e.g. koodisto='sukupuoli' =>
table: d_sukupuoli).

Create the table with default types for columns (see script) and always insert
only rows not yet found (by column koodi) in target table.
"""
import sys,getopt
from time import localtime, strftime

import dboperator

def show(message):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message

def load(schema,table,verbose=False):
  show("begin")

  # selvitä sarakkeet; luupataan kaikki! (voisi parametroida...)
  #dboperator.columns(row)
  # sarakelista koodistosta
  sql = " \n".join([
    "SELECT *",
    "FROM "+schema+".d_"+table+"",
    "WHERE 1=0" # limit to 0
  ])
  try:
    dboperator.execute(sql)
  except:
    # lopetus vai taulun luonti?
    """
    show("Target table does not exist. Create the table first. Over and out.")
    dboperator.close()
    exit(2) # lopeta virheeseen?
    #"""

    #"""
    # koodistosta (vakioiset sarakkeet)
    show("Target table does not exist. Creating it.")
    sql = " \n".join([
      "IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='"+schema+"' AND TABLE_NAME='d_"+table+"') BEGIN",
      "CREATE TABLE "+schema+".d_"+table+" (",
      "id bigint IDENTITY(1,1) NOT NULL",
      #--
      ","+table+"_koodi varchar(100)", # tyyppi?
      ","+table+" nvarchar(255)", # tyyppi?
      ","+table+"_sv nvarchar(255)", # tyyppi?
      ","+table+"_en nvarchar(255)", # tyyppi?
      #--
      ",startdate date",
      ",enddate date",
      #--
      ",loadtime datetime2(4) NOT NULL",
      ",source nvarchar(255) NULL",
      ",username nvarchar(128) NOT NULL",
      "CONSTRAINT PK__d_"+table+" PRIMARY KEY CLUSTERED (id)",
      ");",
      "ALTER TABLE "+schema+".d_"+table+" ADD CONSTRAINT DF__d_"+table+"__loadtime  DEFAULT (getdate()) FOR loadtime;",
      "ALTER TABLE "+schema+".d_"+table+" ADD CONSTRAINT DF__d_"+table+"__username  DEFAULT (suser_name()) FOR username;",
      "END"
    ])
    dboperator.execute(sql)
    #"""

  # vie puuttuva data
  show("Inserting data that isn't there yet.")
  sql = "\n".join([
    "INSERT INTO "+schema+".d_"+table+" ",
    "("+table+"_koodi,"+table+","+table+"_sv,"+table+"_en,startdate,enddate,source) ",
    "SELECT koodi,nimi,nimi_sv,nimi_en,alkupvm,loppupvm,source ",
    "FROM VIPUNEN_SA.dbo.sa_koodistot ",
    "WHERE koodisto='"+table+"' ",
    "AND koodi NOT IN (SELECT "+table+"_koodi FROM "+schema+".d_"+table+") ",
    "ORDER BY koodi "
  ])
  #if verbose: show("execute %s"%(sql))
  dboperator.execute(sql)
  show("Inserted %d rows"%(dboperator.count))

  dboperator.close()

  show("ready")

def usage():
  print """
usage: dimension.py [-s|--schema <schema>] -t|--table <table> [-v|--verbose]

schema defaults to dbo
"""

def main(argv):
  # muuttujat jotka kerrotaan argumentein
  schema = "dbo"
  table = ""
  verbose = False

  try:
    opts, args = getopt.getopt(argv,"s:t:v",["schema=","table=","verbose"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--schematable"): schema = arg
    elif opt in ("-t", "--table"): table = arg
    elif opt in ("-v", "--verbose"): verbose = True
  if not table:
    usage()
    sys.exit(2)

  load(schema,table,verbose)

if __name__ == "__main__":
    main(sys.argv[1:])
