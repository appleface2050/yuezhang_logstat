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

from conf.settings import HiveConf,SHOULD_UPDATE_ZERO_NEW_USER_RUN_PARTNER_LIST
from lib.utils import time_start
from lib.localcache import mem_cache
from model.stat import Scope,BasicStat,VisitStat,TopNStat
from model.stat import BookStat,BookReferStat,BookTagStat,ProductStat
from model.stat import FactorySumStat
from model.stat import Partner
from model.wap import WapPartner,WapFactory
from model.other import Ebk5BookExpand,Temp

class UpdateEbk5BookExpand():
    '''
    给张冉数据库进行更新
    '''
    def get_all_book_id_in_ebk5(self):
        book_id_list = Ebk5BookExpand.mgr().raw("select book_id from ebk5_book_expand")
        return book_id_list

    def get_sum_stat(self):
        tody  = time_start(datetime.datetime.now(),'day')
        #start6 = datetime.datetime.strptime('2012-06-01','%Y-%m-%d')
        #end6 = datetime.datetime.strptime('2012-07-01','%Y-%m-%d')
        q1 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-06-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-07-01','%Y-%m-%d'),charge_type='')
        q2 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-07-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-08-01','%Y-%m-%d'),charge_type='')
        q3 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-08-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-09-01','%Y-%m-%d'),charge_type='')
        q4 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-09-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-10-01','%Y-%m-%d'),charge_type='')
        q5 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-10-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-11-01','%Y-%m-%d'),charge_type='')
        q6 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-11-01','%Y-%m-%d'),end=datetime.datetime.strptime('2012-12-01','%Y-%m-%d'),charge_type='')
        q7 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2012-12-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-01-01','%Y-%m-%d'),charge_type='')
        q8 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-01-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-02-01','%Y-%m-%d'),charge_type='')
        q9 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-02-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-03-01','%Y-%m-%d'),charge_type='')
        q10 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-03-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-04-01','%Y-%m-%d'),charge_type='')
        q11 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-04-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-05-01','%Y-%m-%d'),charge_type='')
        q12 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-05-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-06-01','%Y-%m-%d'),charge_type='')
        q13 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-06-01','%Y-%m-%d'),end=datetime.datetime.strptime('2013-07-01','%Y-%m-%d'),charge_type='')
        q14 = BookStat.mgr().get_data_by_multi_days(scope_id=1,mode='day',start=datetime.datetime.strptime('2013-07-01','%Y-%m-%d'),end=tody,charge_type='')

        data = [q1,q2,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14]
        #data = [q1,q2]
        for q in data:
            print len(q)

        book_id_list = self.get_all_book_id_in_ebk5()
        #book_id_list = book_id_list[0:10]
        
        res = self.merge_multi_month_data_by_bookid(book_id_list,data)
        return res

    def merge_multi_month_data_by_bookid(self,book_id_list,data):
        print "merging"
        res = []
        for book_id in book_id_list:
            books = {'book_id':0,'pv':0,'uv':0,'fee':0,'pay_down':0,'free_down':0}
            books['book_id'] = book_id['book_id']
            for month in data:
                for book in month: 
                    if books['book_id'] == book['book_id']:  #find book_id
                        books['pv'] += book['pv'] 
                        books['uv'] += book['uv']
                        books['fee'] += book['fee']
                        books['pay_down'] += book['pay_down']
                        books['free_down'] += book['free_down']
                        continue
            print books
            res.append(books)
        return res

    def save_data_in_mysql(self,res): 
        tody = time_start(datetime.datetime.now(),'day')
        for book in res:
            try:
                s = Ebk5BookExpand.mgr(ismaster=1).Q(time=tody).filter(book_id=book['book_id'])[0]
                s.consume_money = book['fee']
                s.book_pv = book['pv']
                s.book_uv = book['uv']
                s.update(unikey='book_id') 
                print "importing %s" % s.book_id
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
       
    def save_all_data_in_tmp(self,res):
        import_num = 0
        tody = time_start(datetime.datetime.now(),'day')
        for book in res:
            try:
                s = Temp.new()
                s.book_id = book['book_id']
                s.pv = book['pv']
                s.uv = book['uv']
                s.fee = book['fee']
                s.pay_down = book['pay_down']
                s.free_down = book['free_down']
                s.save()
                import_num += 0
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def save_all_data_in_ebk5(self):
        import_num = 0
        tody  = time_start(datetime.datetime.now(),'day')
        books = Temp.mgr().get_all_data()
        print books
        for book in books:
            try:
                s = Ebk5BookExpand.mgr(ismaster=1).Q(time=tody).filter(book_id=book['book_id'])[0]   
                s.consume_money = float(book['fee'])
                s.book_pv = book['pv']
                s.book_uv = book['uv']
                s.fee_download_num = book['pay_down']
                s.free_download_num = book['free_down']
                s.update(unikey='book_id')
                import_num += 1
                print "updating %s" % s.book_id
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def get_yest_book_data(self,delta=1):
        tody = time_start(datetime.datetime.now(),'day')
        yest = tody - datetime.timedelta(days=delta)
        q = BookStat.mgr().Q(time=yest).filter(scope_id=1,mode='day',time=yest)
        return q 

    def inc_update_data_in_mysql(self,res): 
        import_num = 0
        tody  = time_start(datetime.datetime.now(),'day')
        for book in res:
            try:
                s = Ebk5BookExpand.mgr(ismaster=1).Q(time=tody).filter(book_id=book['book_id'])[0]
                s.consume_money = float(s.consume_money) + float(book['fee'])
                s.book_pv += book['pv']
                s.book_uv += book['uv']
                s.fee_download_num += book['pay_down']
                s.free_download_num += book['free_down']
                s.update(unikey='book_id') 
                import_num += 1
                print "updating %s" % s.book_id
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

if __name__ == '__main__':
    now = datetime.datetime.now()
    s = UpdateEbk5BookExpand()

#first time, save all data in mysql
#    res = s.get_sum_stat()      #得到所有数据并合并
####    s.save_data_in_mysql(res    #因为太慢所以不用了而是用下面两个方法实现首次全部更新
#    s.save_all_data_in_tmp(res)  #14个月数据存入mysql
#    s.save_all_data_in_ebk5()    #将temp表的数据update到ebk5_book_expand

#every day update data
    res = s.get_yest_book_data(1)
    print s.inc_update_data_in_mysql(res)
    
    print datetime.datetime.now()-now




