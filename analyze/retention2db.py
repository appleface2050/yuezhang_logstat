#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import getopt
import json
import urllib2
import logging
import datetime
import time

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from conf.settings import HiveConf
from lib.utils import time_start
from lib.localcache import mem_cache
from analyze.evaluehive import EValueQuery
from service import Service
from analyze.servicehive import ServiceQuery
from model.stat import RetentionStat
class EValue2db(object):
    '''
    query from hive and import the result to mysql
    '''
    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.org_service = ServiceQuery(conf['host'],conf['port'])

    def normlize_time(self, mode, start):
        assert mode in ('hour','day','week','month')
        start = time_start(start,mode)
        if mode == 'hour':
            end = start + datetime.timedelta(hours=1)
        elif mode == 'day':
            end = start + datetime.timedelta(days=1)
        elif mode == 'week':
            end = start + datetime.timedelta(days=7)
        elif mode == 'month':
            if start.month == 12:
                end = datetime.datetime(start.year+1,1,1)
            else:
                end = datetime.datetime(start.year,start.month+1,1)
        return start,end

    def start_importing(self, mode, now=None, stats=None, partner_ids=None):
        n0 = datetime.datetime.now()
        # normalize time
        if not now:
            now = datetime.datetime.now() - datetime.timedelta(days=1)
        if mode == 'day':
            start = time_start(now,'day') 
        elif mode == 'week':
        #elif mode == 'week' and now.weekday() == 6:
            start = time_start(now,'week') 
            print start
        elif mode == 'month' and now.day == 1:
            start = time_start(now,'month')
        else:
            print '%s %s not supported' % (mode,now)
            return
        # import data
        if not stats or 'retention' in stats:
            self.import_new_user_run_week_retention_data(mode,start)
        print mode,start,'time:',datetime.datetime.now()-n0
        return True

    def import_new_user_run_week_retention_data(self, mode, time):
        '''
        start: start time
        '''
        if mode != 'week':
            return
        #time = time - datetime.timedelta(days=7)
        lstart,lend = self.normlize_time(mode,time)
        llstart = lstart - datetime.timedelta(days=7)
        llend = lend - datetime.timedelta(days=7)
        #print lstart,lend
        #print llstart,llend
        yest = datetime.datetime.now() - datetime.timedelta(days=1)
        import_num = 0
        stat1 = self.org_service.get_last_last_week_new_user_run_stat(llstart,llend,yest)
        print stat1
        stat2 = self.org_service.get_last_week_retention_stat(yest, llstart, llend, lstart, lend)
        print stat2
        if stat1 and stat2:
            new_user_run = stat1[0]
            retention = stat2[0]
        #cur=datetime.datetime.now()
        try:
            stat = RetentionStat.new()
            stat.start_time = lstart
            stat.week_num = time.strftime("%W")
            stat.new_user_run = new_user_run
            stat.retention = retention
            print stat
            stat.save()
            import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

if __name__ == '__main__':
    s = EValue2db({'host':'192.168.0.144','port':10000})
    try:
        opts,args = getopt.getopt(sys.argv[1:],'',['mode=','stats=','start=','end=','partner_ids='])
    except getopt.GetoptError,e:
        logging.error('%s\n',str(e),exc_info=True)
        sys.exit(2)
    mode,stats,start,end,partner_ids = None,None,None,None,None
    for o, a in opts:
        if o == '--mode':
            mode = a
        if o == '--stats':
            stats = a
        if o == '--start':
            start = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--end':
            end = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--partner_ids':
            partner_ids = a
    stats = stats.split(',') if stats else None
    stats = 'retention'
    mode = 'week'
    now = datetime.datetime.now()
    if start and end:
        while start < end:
            print 'start...',mode,start
            s.start_importing(mode,start,stats,partner_ids)
            print 'processed...',mode,start
            start += datetime.timedelta(days=1)
    else:
        s.start_importing(mode,None,stats,partner_ids)
    print mode,datetime.datetime.now()-now
 

    

