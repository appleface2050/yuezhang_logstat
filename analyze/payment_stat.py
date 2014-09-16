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
from model.payment import PaymentRechargeDailyStat,PaymentPayDailyStat,PaymentRechargingDetailStat,PaymentRechargingDetailFinishStat,PaymentBalanceStat
from model.payment import PaymentRechargingAmountOrderStat,PaymentRechargingAmountFinishStat,PaymentRechargingDetailOrderStat,PaymentUserRunStat
from conf.inner_version import INNER_VERSION

class Hive2db(object):
    '''
    query from hive and import the result to mysql
    '''
    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.org_service = ServiceQuery(conf['host'],conf['port'])

    def handle_big_data(self, data):
        data = str(data)
        if 'E' in data:
            tmp = data.strip().split('E')
            num = float(tmp[0])
            e = int(tmp[1])
            return 10**e * num
        else:
            return 


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

        import_num = 0
 
        #balance = self.org_service.get_balance_stat(start)
        #for data in balance:
        #    try:
        #        data = data.strip()
        #        i = data.split('\t')
        #        stat = PaymentBalanceStat.new()
        #        stat.time = start.strftime('%Y-%m-%d')
        #        stat.recharge_balance = self.handle_big_data(i[0])
        #        stat.gift_balance = self.handle_big_data(i[1])
        #        stat.save()
        #        import_num += 1
        #    except Exception,e:
        #        print i
        #        logging.error('%s\n',str(e),exc_info=True)

        recharge_amount_order = self.org_service.get_recharge_amount_order(start)
        print recharge_amount_order
        for data in recharge_amount_order:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentRechargingAmountOrderStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.uv = i[0]
                stat.pv = i[1]
                stat.origin = i[2]
                stat.partner_id = i[3]
                stat.innerver = i[4]
                stat.amount = i[5]
                stat.recharging_type = i[6]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
        
        recharge_amount_finish = self.org_service.get_recharge_amount_finish(start)
        for data in recharge_amount_finish:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentRechargingAmountFinishStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.uv = i[0]
                stat.pv = i[1]
                stat.origin = i[2]
                stat.partner_id = i[3]
                stat.innerver = i[4]
                stat.amount = i[5]
                stat.recharging_type = i[6]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)

        pay = self.org_service.get_pay_stat(start)
        for data in pay:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentPayDailyStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.partner_id = i[0]
                stat.version_name = i[1]
                stat.amount = i[2]
                stat.gift_amount = i[3]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
       
        recharge_detail = self.org_service.get_recharge_detail(start)
        for data in recharge_detail:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentRechargingDetailStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.uv = i[0]
                stat.pv = i[1]
                stat.origin = i[2]
                stat.partner_id = i[3]
                stat.innerver = i[4]
                stat.curbustype = i[5]
                stat.recharging_type = i[6]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
        
        recharge_detail_finish = self.org_service.get_recharge_detail_finish(start)
        for data in recharge_detail_finish:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentRechargingDetailFinishStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.uv = i[0]
                stat.pv = i[1]
                stat.origin = i[2]
                stat.partner_id = i[3]
                stat.innerver = i[4]
                stat.recharging_type = i[5]
                stat.amount = i[6]
                stat.gift_amount = i[7]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
        
        recharge_detail_order = self.org_service.get_recharge_detail_order(start)
        for data in recharge_detail_order:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentRechargingDetailOrderStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.uv = i[0]
                stat.pv = i[1]
                stat.origin = i[2]
                stat.partner_id = i[3]
                stat.innerver = i[4]
                stat.recharging_type = i[5]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
 
       
        user_run = self.org_service.get_user_run_innerver(start)
        for data in user_run:
            try:
                data = data.strip()
                i = data.split('\t')
                stat = PaymentUserRunStat.new()
                stat.time = start.strftime('%Y-%m-%d')
                stat.user_run = i[0]
                stat.partner_id = i[1]
                stat.innerver = i[2]
                stat.save()
                import_num += 1
            except Exception,e:
                print i
                logging.error('%s\n',str(e),exc_info=True)
       


        #1.消费和充值对比
        #recharge = self.org_service.get_recharge_stat(start)
        #for data in recharge:
        #    try: 
        #        data = data.strip()
        #        i = data.split('\t')
        #        stat = PaymentRechargeDailyStat.new()
        #        stat.time = start.strftime('%Y-%m-%d')
        #        stat.partner_id = i[0]
        #        stat.version_name = i[1]
        #        stat.recharge_fee = float(i[2])
        #        stat.save()
        #        import_num += 1
        #    except Exception,e:
        #        print i
        #        logging.error('%s\n',str(e),exc_info=True)


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
 

    

