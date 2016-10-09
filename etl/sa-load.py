#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,getopt,os
import httplib
import pymssql
import json
from time import localtime, strftime

# kantayhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
dbhost = os.getenv("PYMSSQL_TEST_SERVER")
dbname = os.getenv("PYMSSQL_TEST_DATABASE")
dbuser = os.getenv("PYMSSQL_TEST_USERNAME")
dbpass = os.getenv("PYMSSQL_TEST_PASSWORD")

# best practices sarakkeet, joita ei tule lähteestä (vakioinen setti)
columnlistignore = ["id","loadtime","source","username"]
#... ja esim 'source' kannattaa/täytyy lisätä erikseen

def load(secure,hostname,url,table):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"
  
  conn = pymssql.connect(dbhost, dbuser, dbpass, dbname)
  cur = conn.cursor(as_dict=True)
  
  if secure:
    httpconn = httplib.HTTPSConnection(hostname)
  else:
    httpconn = httplib.HTTPConnection(hostname)

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
          if i[k] > 2**31:
            columntypes[str(k)] = 'bigint'
          else:
            columntypes[str(k)] = 'int'
        elif type(i[k]) is bool:
          columntypes[str(k)] = 'bit'
        elif type(i[k]) is float:
          columntypes[str(k)] = 'float'
        sqlcreate_mid += ", %s %s NULL"%(k,columntypes[str(k)])
      # luo taulu, jos ei ole
      cur.execute(sqlcreate_beg+sqlcreate_mid+sqlcreate_end)
      conn.commit()
      # tyhjätään
      print strftime("%Y-%m-%d %H:%M:%S", localtime())+" delete %s" % (table)
      cur.execute("TRUNCATE TABLE [%s]"%(table))
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

def main(argv):
  # muuttujat jotka tulee antaa
  secure = False
  hostname,url,table = "","",""

  try:
    opts, args = getopt.getopt(argv,"sH:u:t:",["hostname=","url=","table="])
  except getopt.GetoptError:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table>'
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--secure"): secure = True
    elif opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
  if not hostname or not url or not table:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table>'
    sys.exit(2)
  
  if not dbhost or not dbname or not dbuser or not dbpass:
    print "Missing database settings. Exiting."
    sys.exit(2)
    
  load(secure,hostname,url,table)
      
if __name__ == "__main__":
    main(sys.argv[1:])
