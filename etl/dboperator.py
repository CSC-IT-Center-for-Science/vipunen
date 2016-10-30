#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,os
import pymssql

# Muodosta tietokantayhteys heti!
# (close-funktiolla pitää muistaa sulkea yhteys)
# Mikäli ympäristömuuttujia ei ole asetettu,
# keskeytetään koko ohjelman suorittaminen!

# kantayhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
dbhost = os.getenv("PYMSSQL_TEST_SERVER")
dbname = os.getenv("PYMSSQL_TEST_DATABASE")
dbuser = os.getenv("PYMSSQL_TEST_USERNAME")
dbpass = os.getenv("PYMSSQL_TEST_PASSWORD")
if not dbhost or not dbname or not dbuser or not dbpass:
  print "Missing database settings. Exiting."
  sys.exit(2)

# globaaleina muuttujina; tieto säilyy kutsusta toiseen
conn = pymssql.connect(dbhost, dbuser, dbpass, dbname)
cur = conn.cursor(as_dict=True)

columntypes = dict()
columnlist = []

# best practices sarakkeet, joita ei tule lähteestä (vakioinen setti)
columnlistignore = ["id","loadtime","source","username"]
#... ja esim 'source' kannattaa/täytyy lisätä erikseen

# columns - selvitä sarakkeet ja tyypit
# - populoi columntypes-muuttujan uniikeilla arvoilla
# - populoi columnlist-muuttujan uniikeilla arvoilla
#   huomioi myös columnlistignore-listan
#
# nb! vain yhdestä rivistä/tietueesta kerrallaan!
#  jos rajapinta jättää null-arvot kokonaan pois vastauksesta
#  (ks. esim virta-jtp julkaisut),  tätä funktiota kannattaa/täytyy
#  kutsua n kertaa jotta kaikki sarakenimet tulevat esiin, missä n on
#  vähintään 1 mutta oma valinta; joskus jopa kaikki sarakkeet voi käydä läpi.
#  ideana on siis, että ennen create ja insert kutsuja voisi luupata
#  vaikka koko datasetin läpi niin, että kaikki mahdolliset sarakkeet
#  on selvillä ja vasta sitten luo taulun ja vie datan...
#
def columns(row,debug=False):
  global columntypes, columnlist, columnlistignore
  for col in row:
    # oletuksena merkkijono. tähän menee myös NoneType
    columntypes[str(col)] = 'nvarchar(255)'
    if type(row[col]) is int:
      if row[col] > 2**31:
        columntypes[str(col)] = 'bigint'
      else:
        columntypes[str(col)] = 'int'
    elif type(row[col]) is bool:
      columntypes[str(col)] = 'bit'
    elif type(row[col]) is float:
      columntypes[str(col)] = 'float'
    if debug: print "dboperator.columns: col:%s, type:%s, columntype:%s" % (col,type(row[col]),columntypes[str(col)])
    # lisää sarakelistaan
    if col not in columnlist:
      columnlist.append(col)
  for ignr in columnlistignore:
    if ignr in columnlist:
      columnlist.remove(ignr)
  if debug: print "dboperator.columns: columnlist="+(",".join(columnlist))

# create - luo taulu jos ei ole ja/tai tyhjennä taulu
# nb! sarakkeet ja saraketyypit tulee tuntea jo (ks. columns)
def create(table,debug=False):
  global conn, cur, columntypes, columnlist
  sqlcreate_beg = """
  IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='"""+table+"""') BEGIN
    CREATE TABLE """+table+"""(
      id bigint IDENTITY(1,1) NOT NULL
  """
  sqlcreate_mid = ""
  sqlcreate_end = """
      ,loadtime datetime2(4) NOT NULL
      ,source nvarchar(255) NULL
      ,username nvarchar(128) NOT NULL
      CONSTRAINT PK__"""+table+""" PRIMARY KEY CLUSTERED (id)
    );
    ALTER TABLE """+table+""" ADD CONSTRAINT DF__"""+table+"""__loadtime  DEFAULT (getdate()) FOR loadtime;
    ALTER TABLE """+table+""" ADD CONSTRAINT DF__"""+table+"""__username  DEFAULT (suser_name()) FOR username;
  END
  """
  for col in columnlist:
    sqlcreate_mid += ", %s %s NULL"%(col,columntypes[str(col)])
  # luo taulu, jos ei ole
  cur.execute(sqlcreate_beg+sqlcreate_mid+sqlcreate_end)
  conn.commit()
  # tyhjätään
  cur.execute("TRUNCATE TABLE [%s]"%(table))
  conn.commit()
  if debug: print "dboperator.create: columnlist="+(",".join(columnlist))

# insert - vie rivin (tietueen) tiedot tauluun
# nb! sarakkeet tulee tuntea jo (ks. columns)
def insert(source,table,row,debug=False):
  global conn, cur, columnlist
  if debug: print "dboperator.insert: columnlist="+(",".join(columnlist))
  columnstr = ",".join(columnlist)
  placeholders = ','.join(['%s' for s in columnlist])

  statement = "INSERT INTO [%s] (%s,source) VALUES (%s,'%s');" % (table,columnstr,placeholders,source)
  cur.execute(statement,tuple([row[c] for c in columnlist]))
  conn.commit()

def close(debug=False):
  global conn, cur
  if debug: print "dboperator.close: closing"
  cur.close()
  conn.close()
