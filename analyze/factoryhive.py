#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import getopt
import datetime
import logging

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from conf.settings import PAGE_TYPE,TOP_TYPE
from analyze import HiveQuery
from service import Service
from model.factory import Factory
from model.recharge import FactorySumStat

class FactoryQuery(HiveQuery):
    def __init__(self, host, port):
        super(FactoryQuery,self).__init__(host,port)
        self.cache = {}

    def reset_cache(self):
        self.cache = {}

    def get_factory_stat(self, start, end, **kargs): 
        all_stats = Service.inst().stat.get_all_factstat('day',start,end)
        return all_stats

if __name__ == '__main__':
    s =  FactoryQuery('192.168.0.150','10000')
    try:
        opts,args = getopt.getopt(sys.argv[1:],'',['start=','end=','version_name=','partner_id='])
    except getopt.GetoptError,e:
        logging.error('%s\n',str(e),exc_info=True)
        sys.exit(2)
    jobtype,start,end,version_name,partner_id = '',None,None,None,None
    for o, a in opts:
        if o == '--start':
            start = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--end':
            end = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--version_name':
            version_name = a
        if o == '--partner_id':
            partner_id = a
    now = datetime.datetime.now()
    end = now 
    start = now - datetime.timedelta(days=1)
    print start
    print end
    kargs = {'version_name':version_name,'partner_id':partner_id}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
#    kargs = {'scope_id':82694,'mode':'week'}
    print '厂商总和',s.get_factory_stat(start,end,**kargs)
    print datetime.datetime.now() - now



