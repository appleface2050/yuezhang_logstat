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
from model.e_value import EValue

class EValueQuery(HiveQuery):
    '''
    query from ana_tempdb.temp_downv6_chptbook_evalu_v2
    '''
    def __init__(self, host, port):
        '''
        init hive client
        '''
        super(EValueQuery,self).__init__(host,port)
        self.cache = {}
    
    def reset_cache(self):
        self.cache = {}

    def get_e_value_count(self):
        '''
        get e-value from ana_tempdb.temp_downv6_chptbook_evalu_v2
        '''
        sql = "SELECT * FROM ana_tempdb.temp_downv6_chptbook_evalu_v2"
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

    def get_evalue_startpoint_cfree_stat(self):
        '''
        get startpoint from temp_downv6_1u1b_cfree_stpoint
        '''
        sql = "SELECT bid,startpoint,count(1) usernum FROM ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon GROUP BY bid,startpoint"
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

    def get_evalue_startpoint_min_cid_stat(self):
        '''
        get e-value startpoint stat from ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1
        '''
        #sql = "select bid,startpoint,count(1)usernum from ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon_v1 group by bid,startpoint"
        sql = "select bid,startpoint,count(1)usernum from ana_tempdb.temp_downv6_1u1b_chpt_stpoint_3mon_v1 group by bid,startpoint"
        self.client.execute(sql) 
        res = self.client.fetchAll()
        return res

    def get_evalue_maxcid_maxfee_stat(self):
        '''
        get e-value maxcid maxfee from ana_tempdb.temp_downv6_chptbook_dim_maxcid_maxfee
        '''
        sql = "select bid,maxcid,sumprice from ana_tempdb.temp_downv6_chptbook_dim_maxcid_maxfee"
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

    def get_e_value_pay_distribute_stat(self):
        '''
        get e-value pay distribute from ana_tempdb.temp_downv6_chptbook_p10_distrib
        '''     
        sql = "select * from ana_tempdb.temp_downv6_chptbook_p10_distrib"
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

    def get_e_value_free_distribute_mid_stat(self):
        '''
        get e-value free distribute from ana_tempdb.temp_downv6_chptbook_freechpt_distrib
        '''
        sql = "select * from ana_tempdb.temp_downv6_chptbook_freechpt_distrib"
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

if __name__ == '__main__':
    s = EValueQuery('192.168.0.150','10000')
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

    kargs = {}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
    #print 'EValue\t', s.get_e_value_count()
    print s.get_evalue_startpoint_min_cid_stat()
    print datetime.datetime.now() - now




