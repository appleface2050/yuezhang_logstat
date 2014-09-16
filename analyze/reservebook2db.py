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
from model.stat import ReserveBook
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
        elif mode == 'week' and now.weekday() == 0:
            start = time_start(now,'week') 
        elif mode == 'month' and now.day == 1:
            start = time_start(now,'month')
        else:
            print '%s %s not supported' % (mode,now)
            return
        # import data
        if not stats or 'reservebook' in stats:
            self.import_reservebook_data(mode,start)
        print mode,start,'time:',datetime.datetime.now()-n0
        return True

    def import_reservebook_data(self, mode, time):
        '''
        mode: hour,day,week,month
        start: start time
        return: one day's user run's a week fee
        '''
        if mode != 'day':
            return
        print time
        start,end = self.normlize_time(mode,time)
        import_num = 0
        reservebook = self.org_service.get_reserve_book_stat(start,end)
        try:
            for line in reservebook:
                line = line.strip()
                data = line.split('\t')
                stat = ReserveBook.new()
                stat.datetime = data[0]
                stat.tds = data[1]
                stat.num = data[2]
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
    stats = 'reservebook'
    mode = 'day'
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
 

    

