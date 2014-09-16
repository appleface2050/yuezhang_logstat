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
from model.e_value import EValue,EValueStartPointMinCid
from model.e_value import EValueStartPointClassify,EValueStartPointAll
from model.e_value import EValueMaxCidMaxFee,EValuePayDistribute,EValueFreeDistributeMid,EValueFreeDistribute

class EValue2db(object):
    '''
    query from hive and import the result to mysql
    '''
    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.evalue = EValueQuery(conf['host'],conf['port'])
        
    def convert_startpoint(self,startpoint):
        out = ""
        if startpoint == 1:
            out = "书城"
        elif startpoint == 21:
            out = "书架"
        elif startpoint == 3:
            out = "打包"
        elif startpoint == 40:
            out = "其他"
        return out

    def add_to_10(self,inlist,book_id):
        dft = {'startpoint': 0 , 'bid': book_id, 'usernum': 0}
        while(len(inlist) < 10 ):
            inlist.append(dft)
        return inlist 

    def start_importing_evalue(self):
        '''
        按章E值
        '''
        res = self.evalue.get_e_value_count()
        import_num = 0
        for i in res:
            arr = i.split('\t')
            try:
                stat = EValue.new()
                stat.usnum = int(arr[0])
                stat.cfee = float(arr[1])
                stat.cpay_user = int(arr[2])
                stat.bid = int(arr[3])
                stat.startpoint = self.convert_startpoint(int(arr[4]))
                stat.cpay_user_percentage = "%.1f" % (float(stat.cpay_user)/float(stat.usnum)*100.0)
                stat.ARPU = "%.2f" % (float(stat.cfee)/float(stat.usnum))
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_startpoint_classify(self):
        '''
        为start_importing_e_value_startpoint提供startpoint和usernum，形成cache
        #SELECT bid,startpoint,count(1) usernum FROM ana_tempdb.temp_downv6_1u1b_cfree_stpoint GROUP BY bid,startpoint
        '''
        res = self.evalue.get_evalue_startpoint_cfree_stat()
        import_num = 0
        for i in res:   
            arr = i.split('\t')
            try:
                stat = EValueStartPointClassify.new()
                stat.bid = int(arr[0])
                stat.startpoint = self.convert_startpoint(int(arr[1]))
                stat.usernum = int(arr[2])
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_startpoint_min_cid_stat(self):
        '''
        最小章节
        #select bid,startpoint,count(1)usernum from ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1 group by bid,startpoint
        '''
        res = self.evalue.get_evalue_startpoint_min_cid_stat()
        import_num = 0
        for i in res:   
            arr = i.split('\t')
            try:
                stat = EValueStartPointMinCid.new()
                stat.bid = int(arr[0])
                stat.startpoint = int(arr[1])
                stat.usernum = int(arr[2])
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_startpoint_all(self):
        '''
        各类型startpoint 和 top10开始章节
        #
        '''
        q = EValueStartPointClassify.mgr().get_all_data()
        cache = {}
        for i in q:
            dft = {'书架':0,'书城':0,'打包':0}
            key = i['bid']
            res = cache.get(key,None)
            if res is None:
                cache[key] = dft
            cache[key][i['startpoint']] = i['usernum'] 
        import_num = 0
        for j in cache:
            sp_stat = EValueStartPointMinCid.mgr().get_top10_startpoint(j)[:] 
            sp_stat = self.add_to_10(sp_stat,j)
            try:
                stat = EValueStartPointAll.new()
                stat.bid = int(j)
                stat.shucheng = int(cache[j]['书城'])
                stat.shujia = int(cache[j]['书架'])
                stat.dabao = int(cache[j]['打包'])
                stat.top1 = str(sp_stat[0]['startpoint']) + ':' + str(sp_stat[0]['usernum'])
                stat.top2 = str(sp_stat[1]['startpoint']) + ':' + str(sp_stat[1]['usernum'])
                stat.top3 = str(sp_stat[2]['startpoint']) + ':' + str(sp_stat[2]['usernum'])
                stat.top4 = str(sp_stat[3]['startpoint']) + ':' + str(sp_stat[3]['usernum'])
                stat.top5 = str(sp_stat[4]['startpoint']) + ':' + str(sp_stat[4]['usernum'])
                stat.top6 = str(sp_stat[5]['startpoint']) + ':' + str(sp_stat[5]['usernum'])
                stat.top7 = str(sp_stat[6]['startpoint']) + ':' + str(sp_stat[6]['usernum'])
                stat.top8 = str(sp_stat[7]['startpoint']) + ':' + str(sp_stat[7]['usernum'])
                stat.top9 = str(sp_stat[8]['startpoint']) + ':' + str(sp_stat[8]['usernum'])
                stat.top10 = str(sp_stat[9]['startpoint']) + ':' + str(sp_stat[9]['usernum'])
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_maxcid_maxfee_stat(self):
        '''
        e-value maxcid maxfee stat
        '''
        res = self.evalue.get_evalue_maxcid_maxfee_stat()
        import_num = 0
        for i in res:   
            arr = i.split('\t')
            try:
                stat = EValueMaxCidMaxFee.new()
                stat.bid = int(arr[0])
                stat.maxcid = int(arr[1])
                stat.maxfee = float(arr[2])
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_pay_distribute_stat(self):
        '''
        e-value pay distribute
        '''
        res = self.evalue.get_e_value_pay_distribute_stat()
        import_num = 0
        for i in res:
            arr = i.split('\t')
            try:
                stat = EValuePayDistribute.new()
                stat.bid = int(arr[0])
                stat.p10 = float(arr[1])
                stat.p20 = float(arr[2])
                stat.p30 = float(arr[3])
                stat.p40 = float(arr[4])
                stat.p50 = float(arr[5])
                stat.p60 = float(arr[6])
                stat.p70 = float(arr[7])
                stat.p80 = float(arr[8])
                stat.p90 = float(arr[9])
                stat.p10_usnum = int(arr[10])
                stat.p20_usnum = int(arr[11])
                stat.p30_usnum = int(arr[12])
                stat.p40_usnum = int(arr[13])
                stat.p50_usnum = int(arr[14])
                stat.p60_usnum = int(arr[15])
                stat.p70_usnum = int(arr[16])
                stat.p80_usnum = int(arr[17])
                stat.p90_usnum = int(arr[18])
                stat.p100_usnum = arr[19]
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_free_distribute_mid_stat(self):
        '''
        e-value free distribute middle stat
        '''
        res = self.evalue.get_e_value_free_distribute_mid_stat()
        import_num = 0
        for i in res:
            arr = i.split('\t')
            try:
                stat = EValueFreeDistributeMid.new()
                stat.bid = int(arr[2])
                stat.cidgroup = int(arr[1])
                stat.usernum = int(arr[0])
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing_e_value_free_distribute_stat(self):
        '''
        e-value free distribute
        '''
        #{'bid': 10018812L, 'stat': {'1': 85L, '10': 584L, '-100': 1184L, '30': 107L, '20': 226L}}
        res = EValueFreeDistributeMid.mgr().get_all_data()
        import_num = 0
        for i in res:
            try:
                stat = EValueFreeDistribute.new()
                stat.bid = i['bid']
                stat.d_1 = i['stat']['1']
                stat.d_10 = i['stat']['10']
                stat.d_20 = i['stat']['20']
                stat.d_30 = i['stat']['30']
                stat.d_minue_100 = i['stat']['-100']
                #print stat
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num


if __name__ == '__main__':
    s = EValue2db({'host':'192.168.0.150','port':10000})
    now = datetime.datetime.now()
    print 'Start importing evalue...'
    ##按章E值
    EValue.new().truncate_table()
    s.start_importing_evalue()
    
    ##startpoint stat
    EValueStartPointClassify.new().truncate_table()
    s.start_importing_e_value_startpoint_classify()
    EValueStartPointMinCid.new().truncate_table()
    s.start_importing_e_value_startpoint_min_cid_stat()
    EValueStartPointAll.new().truncate_table()
    s.start_importing_e_value_startpoint_all()

    ##MaxCid MaxFee stat
    #s.start_importing_e_value_maxcid_maxfee_stat()

    ##pay distribute stat
    #s.start_importing_e_value_pay_distribute_stat()
    ##free distribute stawt
    #s.start_importing_e_value_free_distribute_mid_stat()
    #s.start_importing_e_value_free_distribute_stat()

    print 'Finish...time-consumed:',
    print datetime.datetime.now()-now
    
    

