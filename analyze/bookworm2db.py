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
from model.stat import BookWorm
from conf.inner_version import INNER_VERSION

class Hive2db(object):
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
        if not stats or '2db' in stats:
            self.import_data(mode,start)
        print mode,start,'time:',datetime.datetime.now()-n0
        return True

    def import_data(self, mode, time):
        '''
        mode: hour,day,week,month
        start: start time
        return: one day's user run's a week fee
        '''
        if mode != 'day':
            return
        print time
        start,end = self.normlize_time(mode,time)
        pvuv = self.org_service.get_bookworm_pvuv_stat(start)[0]
        right_pvuv = self.org_service.get_bookworm_right_pvuv_stat(start)[0]
        wrong_pvuv = self.org_service.get_bookworm_wrong_pvuv_stat(start)[0]
        pay_pvuv = self.org_service.get_bookworm_pay_pvuv_stat(start)[0]
        recharge_pvuv = self.org_service.get_bookworm_recharge_pvuv_stat(start)[0]
        amount_stat = self.org_service.get_bookworm_amount_stat(start)[0]
        count1 = self.org_service.get_bookworm_pay_count_1(start)[0]
        count2 = self.org_service.get_bookworm_pay_count_2(start)[0]
        count3 = self.org_service.get_bookworm_pay_count_3(start)[0]
        import_num = 0
        try: 
            pvuv = pvuv.strip()
            right_pvuv = right_pvuv.strip()
            wrong_pvuv = wrong_pvuv.strip()
            pay_pvuv = pay_pvuv.strip()
            recharge_pvuv = recharge_pvuv.strip()
            amount_stat = amount_stat.strip()
            pvuv_tmp = pvuv.split('\t')
            wrong_pvuv_tmp = right_pvuv.split('\t')
            right_pvuv_tmp = wrong_pvuv.split('\t')
            pay_pvuv_tmp = pay_pvuv.split('\t')
            recharge_pvuv_tmp = recharge_pvuv.split('\t')
            amount_pvuv_tmp = amount_stat.split('\t')
            stat = BookWorm.new()
            stat.time = start.strftime('%Y-%m-%d')
            stat.pv = int(pvuv_tmp[0])
            stat.uv = int(pvuv_tmp[1])
            stat.right_user = right_pvuv_tmp[0]
            stat.right_num = right_pvuv_tmp[1]
            stat.wrong_user = wrong_pvuv_tmp[0]
            stat.wrong_num = wrong_pvuv_tmp[1]
            stat.pay_user = pay_pvuv_tmp[1] 
            stat.pay_times = pay_pvuv_tmp[0]
            stat.recharge_page_pv = recharge_pvuv_tmp[0]
            stat.recharge_page_uv = recharge_pvuv_tmp[1]
            stat.recharge_user = amount_pvuv_tmp[0]
            stat.amount = amount_pvuv_tmp[1]
            stat.pay_num1 = count1
            stat.pay_num2 = count2
            stat.pay_num3 = count3
            stat.save()
            import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

if __name__ == '__main__':
    s = Hive2db({'host':'192.168.0.144','port':10000})
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
    stats = '2db'
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
 

    

