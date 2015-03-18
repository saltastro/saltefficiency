# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 10:06:20 2015

@author: jpk

ToDo: automate the subsystems check. A query that checks all the subsystems in
case things change in the future should prevent issues with the pis chart
colours

"""

import pandas as pd
import pandas.io.sql as psql
import MySQLdb
import matplotlib.pyplot as pl
import report_queries as rq
import numpy as np
import matplotlib.dates as mdates


def priority_breakdown_pie_chart(x, ds):
    '''
    make a pie chart from the dataframe
    '''
    temp = list(x['Priority'])
    no_blocks = map(int, list(x['No. Blocks']))
    labels = ['P'+str(temp[i])+' - ' + str(no_blocks[i]) for i in range(0,len(temp))]
    values = list(x['Tsec'])

    # set colours for the priorities
    colours = ['b','c','g','m','r']

    fig = pl.figure(facecolor='w', figsize=[5, 5])
    ax = fig.add_subplot(111)
    ax.set_aspect=1

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.95,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'w'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.7), fontsize=8)
    title_txt = 'Weekly Priority Breakdown - ' + str(int(x['No. Blocks'].sum())) + ' Blocks Total' + '\n {}'.format(ds)

    ax.set_title(title_txt, fontsize=12)

    filename = 'priority_breakdown_pie_chart_' +'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(filename, dpi=100)
#    pl.show()

def weekly_total_time_breakdown_pie_chart(x, ds):

    labels = ['Science - {}'.format(x['ScienceTime'][0]),
              'Engineering - {}'.format(x['EngineeringTime'][0]),
              'Weather - {}'.format(x['TimeLostToWeather'][0]),
              'Problems - {}'.format(x['TimeLostToProblems'][0])]

    values = [int(x['Science']),
              int(x['Engineering']),
              int(x['Weather']),
              int(x['Problems'])]

    colours = ['b','c','g','r']

    fig = pl.figure(facecolor='w', figsize=[5, 5])
    ax = fig.add_subplot(111)
    ax.set_aspect=1

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.95,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'w'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.8), fontsize=8)

    title_txt = 'Weekly Time Breakdown - {} Total\n{}'.format(x['NightLength'][0], ds)
    ax.set_title(title_txt, fontsize=12)

    filename = 'weekly_total_time_breakdown_pie_chart_' + '-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(filename, dpi=100)
#    pl.show()

def weekly_subsystem_breakdown_pie_chart(x, y, col_dict, ds):


    subsystem = list(x['SaltSubsystem'])
    time = list(x['TotalTime'])

    labels = [subsystem[i] + ' - ' + time[i] for i in range(0,len(subsystem))]
    values = list(x['Time'])

    colours = [col_dict[i] for i in subsystem]

    fig = pl.figure(facecolor='w', figsize=[5, 5])
    ax = fig.add_subplot(111)
    ax.set_aspect=1

    pie_wedge_collection = ax.pie(values,
                                  colors=colours,
                                  pctdistance=0.8,
                                  radius = 0.95,
                                  autopct='%1.1f%%',
                                  textprops = {'fontsize':10,
                                               'color':'k'},
                                  wedgeprops = {'edgecolor':'white'})

    ax.legend(labels=labels, frameon=False, loc=(-0.15,0.65), fontsize=8)

    title_txt = 'Weekly Problems Breakdown - {}\n{}'.format(y['TotalTime'][0], ds)
    ax.set_title(title_txt, fontsize=12)

    filename = 'weekly_subsystem_breakdown_pie_chart_'+'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(filename, dpi=100)
#    pl.show()

def weekly_time_breakdown(x, ds):
    '''
    produce a bar stacked bar chart plot of the time breakdown per day for the
    past week.
    '''

    fig = pl.figure(figsize=(10,4),facecolor='w')
    ax = fig.add_subplot(111)
    width = 0.55
    ax.grid(which='major', axis='y')
    # science time per day
    s = ax.bar(x['Date'],
                x['Science'],
                width,
                color = 'b',
                edgecolor='w')

    # engineering time per day
    e = ax.bar(x['Date'],
               x['Engineering'],
                width,
                bottom = x['Science'],
                color = 'c',
                edgecolor='w')

    # weather time per day
    w = ax.bar(x['Date'],
               x['Weather'],
                width,
                bottom = x['Science'] + x['Engineering'],
                color = 'g',
                edgecolor='w')

    # problem time per day
    p = ax.bar(x['Date'],
               x['Problems'],
                width,
                bottom = x['Science'] + x['Engineering'] + x['Weather'],
                color = 'r',
                edgecolor='w')


    ax.set_ylabel('Hours', fontsize=11)
    ax.set_xlabel('Date', fontsize=11)
    fig.legend((s[0], e[0], w[0], p[0]),
                   ('Science Time',
                    'Engineering Time',
                    'Time lost to Weather',
                    'Time lost to Problems'),
                    frameon=False,
                    fontsize=10,
                    loc=(0.0,0.70))

    title_txt = 'Weekly Time Breakdown - {}'.format(ds)

    ax.set_title(title_txt, fontsize=11)
    ax.xaxis_date()
    date_formatter = mdates.DateFormatter('%a \n %Y-%m-%d')
    ax.xaxis.set_major_formatter(date_formatter)

    for tick in ax.xaxis.get_major_ticks():
                tick.label.set_fontsize(8)
    for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(8)

    fig.autofmt_xdate(rotation=0, ha = 'left')
    fig.subplots_adjust(left=0.22, bottom=0.20, right=0.96, top=None,
                      wspace=None, hspace=None)
    pl.autoscale()
    filename = 'weekly_time_breakdown_'+'-'.join([ds.split()[0].replace('-',''), ds.split()[2].replace('-','')])+'.png'
    pl.savefig(filename, dpi=100)
#    pl.show()


if __name__=='__main__':
    import os
    # set the colours for all the subsystems:
    subsystems_list = ['BMS', 'DOME', 'TC', 'PMAS', 'SCAM', 'TCS', 'STRUCT',
                       'TPC', 'HRS', 'PFIS','Proposal', 'Operations',
                       'ELS', 'ESKOM']
    cmap = pl.cm.jet
    colour_map = cmap(np.linspace(0.0, 1.0, len(subsystems_list)))
    col_dict = {}

    for i in range(0, len(subsystems_list)):
        col_dict[subsystems_list[i]] = colour_map[i]


    # open mysql connection to the sdb
    mysql_con = MySQLdb.connect(host='sdb.cape.saao.ac.za',
                port=3306,user=os.environ['SDBUSER'], 
                passwd=os.environ['SDBPASS'], db='sdb')

    date = '2015-03-16'
    interval = 7

    # use the connection to get the required data: _d
    dr_d = rq.date_range(mysql_con, date, interval=7)
    wpb_d = rq.weekly_priority_breakdown(mysql_con, date, interval=7)
    wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=7)
    wttb_d = rq.weekly_total_time_breakdown(mysql_con, date, interval=7)
    wsb_d = rq.weekly_subsystem_breakdown(mysql_con, date, interval=7)
    wsbt_d = rq.weekly_subsystem_breakdown_total(mysql_con, date, interval=7)
    wtb_d = rq.weekly_time_breakdown(mysql_con, date, interval=7)

    date_string = '{} - {}'.format(dr_d['StartDate'][0], dr_d['EndDate'][0])

    # testing the pie_chart method
    priority_breakdown_pie_chart(wpb_d, date_string)
    weekly_total_time_breakdown_pie_chart(wttb_d, date_string)
    weekly_subsystem_breakdown_pie_chart(wsb_d, wsbt_d, col_dict, date_string)
    weekly_time_breakdown(wtb_d, date_string)

    mysql_con.close()

