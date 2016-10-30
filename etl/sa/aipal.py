#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,getopt,os
import httplib,base64
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

# aipal-api yhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
apiuser = os.getenv("AIPAL_USERNAME")
apipass = os.getenv("AIPAL_PASSWORD")

def load(secure,hostname,url,table,begin,end,debug):
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" start"
  
  conn = pymssql.connect(dbhost, dbuser, dbpass, dbname)
  cur = conn.cursor(as_dict=True)
  
  if secure:
    httpconn = httplib.HTTPSConnection(hostname)
  else:
    httpconn = httplib.HTTPConnection(hostname)

  headers={'Content-Type': 'application/json', 'Authorization': 'Basic %s' % base64.b64encode(apiuser+":"+apipass)}
  if debug: print "headers=",headers
  
  if not begin: begin = "%d-%02d-01" % (localtime().tm_year,(localtime().tm_mon-1))
  if not end: end = strftime("%Y-%m-%d",localtime())
  data='{"alkupvm": "%s", "loppupvm": "%s"}' % (begin,end)
  if debug: print "data=",data
  
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url
  httpconn.request('POST', url, data, headers)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" got %d " % (len(j))
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
        columntypes[str(k)] = 'nvarchar(max)'
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
      print strftime("%Y-%m-%d %H:%M:%S", localtime())+" create if not exists %s" % (table)
      # luo taulu, jos ei ole
      cur.execute(sqlcreate_beg+sqlcreate_mid+sqlcreate_end)
      conn.commit()
      
      # tyhjätään
      #cur.execute("TRUNCATE TABLE [%s]"%(table))
      #conn.commit()
      # NB NB NB
      # TÄSSÄ ON NIMETTY SARAKE LÄHTEESTÄ JA SA-TAULUSTA
      sql = "DELETE FROM [%s] WHERE vastausaika>='%s'"%(table,begin)
      print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %s" % (sql)
      cur.execute(sql)
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
  hostname,url,table,begin,end,debug = "","","","","",0

  try:
    opts, args = getopt.getopt(argv,"sH:u:t:bed",["hostname=","url=","table=","begin=","end=","debug"])
  except getopt.GetoptError:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-b|--begin <date>] [-e|--end <date>] [-d|--debug]'
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-s", "--secure"): secure = True
    elif opt in ("-H", "--hostname"): hostname = arg
    elif opt in ("-u", "--url"): url = arg
    elif opt in ("-t", "--table"): table = arg
    elif opt in ("-b", "--begin"): begin = arg
    elif opt in ("-e", "--end"): end = arg
    elif opt in ("-d", "--debug"): debug = 1
  if not hostname or not url or not table:
    print ' [-s] -H|--hostname <hostname> -u|--url <url> -t|--table <table> [-b|--begin <date>] [-e|--end <date>] [-d|--debug]'
    sys.exit(2)
  
  if debug: print "debugging"
  
  if not dbhost or not dbname or not dbuser or not dbpass:
    print "Missing database settings. Exiting."
    sys.exit(2)
  if not apiuser or not apipass:
    print "Missing api settings. Exiting."
    sys.exit(2)
    
  load(secure,hostname,url,table,begin,end,debug)
      
if __name__ == "__main__":
    main(sys.argv[1:])
