import os
import sys, time, datetime, string
import struct
import numpy as np
import mysql

from blockvisitstats import blockvisitstats

if __name__=='__main__':
   elshost='db2.suth.saao.ac.za'
   elsname='els'
   elsuser=os.environ['ELSUSER']
   elspassword=os.environ['SDBPASS']
   sdbhost='sdb.salt'
   sdbname='sdb'
   user=os.environ['SDBUSER']
   password=os.environ['SDBPASS']

   sdb=mysql.mysql(sdbhost, sdbname, user, password, port=3306)

   sdate = sys.argv[1]
   edate=sys.argv[2]
   sdate = datetime.datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
   edate = datetime.datetime(int(edate[0:4]), int(edate[4:6]), int(edate[6:8]))
   date = sdate
   problem_dict={}
   while date <= edate:
       obsdate = '%4i%2s%2s' % (date.year, string.zfill(date.month, 2), string.zfill(date.day,2))
       print obsdate
       results=blockvisitstats(sdb, obsdate, update=True)
       date += datetime.timedelta(days=1)

