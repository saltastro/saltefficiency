# -*- coding: utf-8 -*-
"""

This script produces an html page for observations taken on a given observing
date at SALT

"""

import os
import sys
import pandas as pd
import pandas.io.sql as psql
import MySQLdb
import matplotlib.pyplot as pl
import numpy as np
from datetime import datetime

import mysql
import report_queries as rq
import sdb_utils as su

def night_summary_page(obsdate, sdb, els, dirname='./logs/'):
    """Create a summary for the given observing date

    Parameters
    ----------
    obsdate: str
       Observing date in YYYYMMDD format

    dirname: str
       Default directory to write results

    sdb: ~mysql.mysql
       A connection to the sdb database

    els: ~mysql.mysql
       A connection to the els database


    """
    night_txt=''

    # display the night information
    nid = su.getnightinfo(sdb, obsdate)
    logic = 'NightInfo_Id={}'.format(nid)
    sa = sdb.select('Surname', 'NightInfo join Investigator on SA_Id=Investigator_Id', logic)[0][0]
    so = sdb.select('Surname', 'NightInfo join SaltOperator on SO1_Id=SO_Id', logic)[0][0]
    ct = sdb.select('Surname', 'NightInfo join Investigator on CTDuty_Id=Investigator_Id', logic)[0][0]
    info_txt = """
<h2> Night Summary for {0} </h2> 
<div>
   SA: {1} <br> 
   SO: {2} <br> 
   CT: {3} <br> 
</div>""".format(obsdate, sa, so, ct)

    sel_times = 'ScienceTime, EngineeringTime, TimeLostToWeather, TimeLostToProblems'
    night_times = sdb.select(sel_times, 'NightInfo', logic)[0]
    info_txt += """\n
<div>
<h3> Night Statistics </h3>
    Science Time: {0:.2f} <br>
    Engineering Time: {1:.2f} <br>
    Time Lost to Weather: {2:.2f} <br>
    Time Lost to Problems: {3:.2f} <br>
</div>\n""".format(night_times[0]/3600.0, night_times[1]/3600.0, night_times[2]/3600.0, night_times[3]/3600.0,)

    night_txt += info_txt

    # display the night break down
    break_txt = '<h3> Night Breakdown</h2>'
    #break_txt += create_night_table(obsdate, sdb, els)
  
    night_txt += break_txt
    # add a list of accecpted blocks

    # add a list of proposals and files for each proposal

    #write the results to the output
    write_night_report_to_file(obsdate, night_txt, dirname)
    


def write_night_report_to_file(obsdate, txt, dirname='./logs/'):
    """This function writes the text to a file and names the report accorting
    to the date range specified

    Parameters
    ----------
    obsdate: str
       Observing date in YYYYMMDD format

    txt: str
       Report to right to file

    dirname: str
       Default directory to write results
    """


    header = """<html>
<head><title>SALT Night Report for {0}</title></head>
<body bgcolor="white" text="black" link="blue" vlink="blue">\n
    """.format(obsdate)

    bottom="""\n<br><center> Updated: {0} </center>
              </body> 
              </hmtl>""".format(datetime.now().strftime('%Y-%m-%d  %H:%M:%S')) 
    html_txt = header+txt + bottom
    filename = 'night_report_{0}.html'.format(obsdate)

    with open(dirname+filename, 'w') as f:
        f.write(html_txt)

if __name__=='__main__':

    # open mysql connection to the sdb
    mysql_con = MySQLdb.connect(host='sdb.cape.saao.ac.za',
                port=3306, user=os.environ['SDBUSER'], 
                passwd=os.environ['SDBPASS'], db='sdb')

    sdbhost='sdb.salt'
    sdbname='sdb'
    user=os.environ['SDBUSER']
    password=os.environ['SDBPASS']
    sdb=mysql.mysql(sdbhost, sdbname, user, password, port=3306)


    obsdate = sys.argv[1]
    date = '{}-{}-{}'.format(obsdate[0:4], obsdate[4:6], obsdate[6:8])
    interval=1

    # use the connection to get the required data: _d
    #dr_d = rq.date_range(mysql_con, date, interval=interval)
    #wpb_d = rq.weekly_priority_breakdown(mysql_con, date, interval=interval)
    #wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=interval)
    #wttb_d = rq.weekly_total_time_breakdown(mysql_con, date, interval=interval)
    #wsb_d = rq.weekly_subsystem_breakdown(mysql_con, date, interval=interval)

    # TESTING: save the dataframes
    #dr_d.save('dr_d')
    #wpb_d.save('wpd_d')
    #wtb_d.save('wtb_d')
    #wttb_d.save('wttd_d')
    #wsb_d.save('wsb_d')

    #create the page
    night_summary_page(obsdate, sdb, mysql_con)

    mysql_con.close()
