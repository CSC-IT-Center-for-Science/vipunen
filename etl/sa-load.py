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

def load(secure,hostname,url,table,debug):
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
    # selvitä sarakkeet, luo taulu jos ei ole ja/tai tyhjennä taulu
    # nb! vain ensimmäisestä palautetusta tiedosta, joten jos rajapinta
    #     jättää null-arvot kokonaan pois vastauksesta,
    #     tämä skripti ei toimi! (ks. esim virta-jtp julkaisut)
    if cnt == 1:
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
      print strftime("%Y-%m-%d %H:%M:%S", localtime())+" create and/or truncate %s" % (table)
      # luo taulu, jos ei ole
      cur.execute(sqlcreate_beg+sqlcreate_mid+sqlcreate_end)
      conn.commit()
      # tyhjätään
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

    if debug: print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %s"%(cnt)
    statement = "INSERT INTO [%s] (%s,source) VALUES (%s,'%s');" % (table,columnstr,placeholders,hostname+url)
    cur.execute(statement,tuple([i[s] for s in columnlist]))
    conn.commit()
      
  cur.close()
  conn.close()
  
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"

def main(argv):
  # muuttujat jotka tulee antaa
  secure = False
  hostname,url,table,debug = "","","",0

  try:
    opts, args = getopt.getopt(argv,"sH:u:t:d",["hostname=","url=","table=","debug"])
  except getopt.GetoptError:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-d|--debug]'
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--secure"): secure = True
    elif opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
    elif opt in ("-d", "--debug"): debug = 1
  if not hostname or not url or not table:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-d|--debug]'
    sys.exit(2)
  
  if debug: print "debugging"
  
  if not dbhost or not dbname or not dbuser or not dbpass:
    print "Missing database settings. Exiting."
    sys.exit(2)
    
  load(secure,hostname,url,table,debug)
      
if __name__ == "__main__":
    main(sys.argv[1:])
