#!/usr/bin/python
# vim: set fileencoding=UTF-8 :
import sys,os
import httplib
import pymssql
import json
from time import localtime, strftime

# muuttujat jotka tulee vaihtaa lähteen vaihtuessa
hostname = "amkoutetestapp.csc.fi"
url = "/api/public/valos"
table = "sa_valos"

# kantayhteysmuuttujat ympäristöasetuksista! (onhan asetettu?)
server = os.getenv("PYMSSQL_TEST_SERVER")
database = os.getenv("PYMSSQL_TEST_DATABASE")
user = os.getenv("PYMSSQL_TEST_USERNAME")
password = os.getenv("PYMSSQL_TEST_PASSWORD")

# best practices sarakkeet, joita ei tule lähteestä (vakioinen setti)
columnlistignore = ["id","loadtime","source","username"]
#... ja esim 'source' kannattaa/täytyy lisätä erikseen

def main():
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" begin"

  #print "Connecting to database %s" % (database)
  conn = pymssql.connect(server, user, password, database)
  cur = conn.cursor(as_dict=True)

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" delete %s" % (table)
  cur.execute("DELETE FROM [%s]"%(table))
  conn.commit()

  cur.execute("SELECT TOP 0 * FROM [%s]"%(table))
  columnlist = [desc[0] for desc in cur.description]
  for ignr in columnlistignore:
    if ignr in columnlist:
      columnlist.remove(ignr)
  columnstr = ",".join(columnlist)
  placeholders = ','.join(['%s' for s in columnlist])

  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" load from "+hostname+url
  httpconn = httplib.HTTPSConnection(hostname)
  httpconn.request('GET', url)
  r = httpconn.getresponse()
  j = json.loads(r.read())
  cnt = 0
  for i in j:
    cnt += 1
    #print strftime("%Y-%m-%d %H:%M:%S", localtime())+" %s"%(cnt)
    statement = "INSERT INTO [%s] (%s,source) VALUES (%s,'%s');" % (table,columnstr,placeholders,hostname+url)
    cur.execute(statement,tuple([i[s] for s in columnlist]))
    conn.commit()
      
  cur.close()
  conn.close()
  
  print strftime("%Y-%m-%d %H:%M:%S", localtime())+" ready"
    
if __name__ == "__main__":
    main()
