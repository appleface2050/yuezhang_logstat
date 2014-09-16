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

import IP

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from conf.settings import HiveConf
from lib.utils import time_start
from lib.localcache import mem_cache
from analyze.evaluehive import EValueQuery
from service import Service
from analyze.servicehive import ServiceQuery

class IPProvince(object):
    '''
    ip and province 2 hive
    '''
    def find_ip(self,ip):
        data = IP.find(ip)
        if data:
            try:
                province = data.strip().split('\t')
                if len(province) == 2:
                    return province[1]
                else:
                    return province[0]
            except Exception, e:
                print data
        else:
            return "其他"

class Hive2db(object):
    '''
    query from hive and import the result to mysql
    '''
    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.org_service = ServiceQuery(conf['host'],conf['port'])
        self.ip_finder = IPProvince()

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
        output_file = open('ip_province.txt', 'w')
        output = ""
        if mode != 'day':
            return
        print time
        start,end = self.normlize_time(mode,time)
        try:    
            import_num = 0
            ips = self.org_service.get_new_ips(start,end)
            #ips = self.org_service.get_all_ips(start,end)
            #ips = ['www.baidu.com','www.alipay.com']
            print "finding %s ips..." % str(len(ips))
            for ip in ips:
                ip = str(ip.strip())
                province = self.ip_finder.find_ip(ip)
                tmp = ip + '\t' + province + '\n'
                output += tmp
            output = output.encode('utf-8')

            output_file.write(output)
            output_file.close()
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
 

 

#if __name__ == "__main__":
#    ip = IPProvince()
#    print ip.find_ip('www.baidu.com')





