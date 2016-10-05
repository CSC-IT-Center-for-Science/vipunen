#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,getopt,os
import httplib
import pymssql
import json
from time import localtime, strftime

# kantayhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

# best practices sarakkeet, joita ei tule lähteestä (vakioinen setti)
columnlistignore = ["id","loadtime","source","username"]
#... ja esim 'source' kannattaa/täytyy lisätä erikseen

def main(argv):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"
  
  # muuttujat jotka tulee antaa
  hostname = ""
  url = ""
  table = ""

  try:
    opts, args = getopt.getopt(argv,"H:u:t:",["hostname=","url=","table="])
  except getopt.GetoptError:
    print ' -H <hostname> -u <url> -t <table>'
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
  if not hostname: sys.exit(2)
  if not url: sys.exit(2)
  if not table: sys.exit(2)
  
  #print "Connecting to database %s" % (database)
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor(as_dict=True)

  httpconn = httplib.HTTPSConnection(hostname)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url
  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  cnt = 0
  columntypes = dict()
  columnlist = []
  columnstr = ""
  placeholders = ""
  for i in j:
    cnt += 1
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
    # selvitä sarakkeet
    if cnt == 1:
      for k in i:
        # oletuksena merkkijono. tähän menee myös NoneType
        columntypes[str(k)] = 'nvarchar(255)'
        if type(i[k]) is int:
          columntypes[str(k)] = 'int'
        elif type(i[k]) is bool:
          columntypes[str(k)] = 'bit'
        sqlcreate_mid += ", %s %s NULL"%(k,columntypes[str(k)])
      # luo taulu, jos ei ole
      cur.execute(sqlcreate_beg+sqlcreate_mid+sqlcreate_end)
      conn.commit()
      # tyhjätään
      print strftime("%Y-%m-%d %H:%M:%S", localtime())+" delete %s" % (table)
      cur.execute("DELETE FROM [%s]"%(table))
      conn.commit()
      # tee sarakelistat
      cur.execute("SELECT TOP 0 * FROM [%s]"%(table))
      columnlist = [desc[0] for desc in cur.description]
      for ignr in columnlistignore:
        if ignr in columnlist:
          columnlist.remove(ignr)
      columnstr = ",".join(columnlist)
      placeholders = ','.join(['%s' for s in columnlist])

    #print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %s"%(cnt)
    statement = "INSERT INTO [%s] (%s,source) VALUES (%s,'%s');" % (table,columnstr,placeholders,hostname+url)
    cur.execute(statement,tuple([i[s] for s in columnlist]))
    conn.commit()
      
  cur.close()
  conn.close()
  
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"
    
if __name__ == "__main__":
    main(sys.argv[1:])
