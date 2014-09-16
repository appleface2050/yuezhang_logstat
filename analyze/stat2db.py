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

from conf.settings import HiveConf,SHOULD_UPDATE_ZERO_NEW_USER_RUN_PARTNER_LIST,DO_NOT_UPDATE_NEW_USER_RUN_PARTNERID_LIST
from lib.utils import time_start
from lib.localcache import mem_cache
from analyze.mysql_data import DownloadQuery as NewDownloadQuery
from analyze.mysql_data import ServiceQuery as NewServiceQuery
from analyze.downloadhive import DownloadQuery
from analyze.servicehive import ServiceQuery
from analyze.factoryhive import FactoryQuery
from analyze.waphive import WapQuery
from service import Service
from model.stat import Scope,BasicStat,VisitStat,TopNStat,Partner,BasicStatv3
from model.stat import BookStat,BookReferStat,BookTagStat,ProductStat
from model.stat import FactorySumStat,ArpuOneWeekFee,TemparpuOneWeekNewUserVisit,Arpu7DaysArpuStat
from model.wap import WapPartner,WapFactory

class Stat2db(object):
    '''
    query from hive and import the result to mysql
    '''
    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.down = NewDownloadQuery()
        self.service = NewServiceQuery()
        self.org_down = DownloadQuery(conf['host'],conf['port'])    
        self.org_service = ServiceQuery(conf['host'],conf['port'])  
        self.factory = FactoryQuery(conf['host'],conf['port'])
        self.wap = WapQuery(conf['host'],conf['port'])
        self.is_factory_sum_stat_done = False

    def get_scope(self, **kargs):
        '''
        get scope according to params
        kargs:platform_id,run_id,plan_id,partner_id,version_name,product_name
        '''
        platform_id = kargs.get('platform_id',None)
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id= kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        # Query
        q = Scope.mgr().Q()
        platform_id and q.filter(platform_id=platform_id)
        run_id and q.filter(run_id=run_id)
        plan_id and q.filter(plan_id=plan_id)
        partner_id and q.filter(partner_id=partner_id)
        version_name and q.filter(version_name=version_name)
        product_name and q.filter(product_name=product_name)
        return q[0]

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
   
    def import_partner_productname(self, mode, start):
        if mode != 'day':
            return 0
        print "start importing new scope"
        start,end = self.normlize_time(mode,start)
        res = self.service.get_partner_product(start,end)
        #res = self.org_service.get_partner_product(start,end)
        for i in res:
            if not (i.isdigit() and int(i)<1000000 and int(i)>1000):
                continue
            try:
                q = Scope.new().Q().filter(partner_id=i,product_name='',run_id='',version_name='',plan_id='')
                q.filter(platform_id=6)
                if not q[0]:
                    s = Scope.new()
                    s.platform_id = 6
                    s.partner_id = i
                    s.save()
                for j in res[i]:
                    if re.search(r'^[\s0-9a-zA-Z+()_-]+$',j):
                        q = Scope.new().Q().filter(partner_id=i,product_name=j,run_id='',version_name='',plan_id='')
                        q.filter(platform_id=6)
                        if not q[0]:
                            s = Scope.new()
                            s.platform_id = 6
                            s.partner_id = i
                            s.product_name = j
                            s.save()
                        q = Scope.new().Q().filter(partner_id='',product_name=j,run_id='',version_name='',plan_id='')
                        q.filter(platform_id=6)
                        if not q[0]:
                            s = Scope.new()
                            s.platform_id = 6
                            s.product_name = j
                            s.save()
                for j in ['ireader_1.6','ireader_1.7','ireader_1.8','ireader_2.0','ireader_2.1','ireader_2.2','ireader_2.3','ireader_2.4','ireader_2.6','ireader_2.7','ireader_3.0','ireader_3.1','ireader_3.2']:
                    q = Scope.new().Q().filter(partner_id=i,product_name='',run_id='',version_name=j,plan_id='')
                    q.filter(platform_id=6)
                    if not q[0]:
                        s = Scope.new()
                        s.platform_id = 6
                        s.partner_id = i
                        s.version_name = j
                        s.save()
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return len(res)
     
    def import_basic_stat(self, mode, start, **kargs):
        '''
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return None
        start,end = self.normlize_time(mode,start)
        s = BasicStat.new()
        try:
            s.scope_id = scope['id']
            s.mode = mode
            s.time = start
            s.visits = self.service.get_pv(start,end,**kargs)
            s.imei = 0
            s.user_run = self.org_service.get_run_user_count(start,end,**kargs)
            s.new_user_run = self.org_service.get_new_runuser_count(start,end,**kargs)
            s.user_visit = self.service.get_vis_user_count(start,end,**kargs)
            s.new_user_visit = self.org_service.get_new_visuser_count(start,end,**kargs)
            s.active_user_visit = self.service.get_active_user_count(start,end,**kargs)
            if mode in ('week'):
                if scope['id'] == 1:
                    counttype = 'ALL'
                if scope['id'] in (39,40,41,42):
                    counttype = 'VERSION'
                else:
                    counttype = ''
                s.user_retention = self.service.get_retention_user_count(mode,counttype,start,end,**kargs)
            s.pay_user = self.down.get_pay_user_count(start,end,**kargs)
            s.cpay_down = self.down.get_downcount(start,end,'chapter',ispay=True,**kargs)
            s.cfree_down = self.down.get_downcount(start,end,'chapter',**kargs)
            s.bpay_down = self.down.get_downcount(start,end,'book',ispay=True,**kargs)
            s.bfree_down = self.down.get_downcount(start,end,'book',**kargs)
            s.cpay_user = self.down.get_downcount(start,end,'chapter',ispay=True,is_user_unique=True,**kargs)
            s.cfree_user = self.down.get_downcount(start,end,'chapter',is_user_unique=True,**kargs)
            s.bpay_user = self.down.get_downcount(start,end,'book',ispay=True,is_user_unique=True,**kargs)
            s.bfree_user = self.down.get_downcount(start,end,'book',is_user_unique=True,**kargs)
            batch = self.down.get_batch_basic(start, end, **kargs)
            s.batch_pv = batch['batch_pv']
            s.batch_uv = batch['batch_uv']
            s.batch_fee = batch['batch_fee']
            if mode in ('week','month','year'):
                sum_byday = BasicStat.mgr().get_data(s.scope_id,'day',start,end)
                s.cfee,s.bfee = sum_byday['cfee'],sum_byday['bfee']
            else:
                s.cfee = self.down.get_fee_by_chapter(start,end,**kargs)
                s.bfee = self.down.get_fee_by_book(start,end,**kargs)
            s.recharge = self.down.get_recharge(start,end,**kargs)
            if s.visits or s.imei or s.user_run or s.user_visit or s.active_user_visit or s.cpay_down or s.cfree_down or s.bpay_down or s.bfree_down:
                s.save()
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return s

    def patch_basic(self, mode, start, **kargs):
        '''
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return None
        start,end = self.normlize_time(mode,start)
        try:
            s = BasicStat.mgr(ismaster=1).Q(time=start).filter(scope_id=scope['id'],mode=mode,time=start)[0]
            if s:
                #s.visits = self.service.get_pv(start,end,**kargs)
                #s.imei = 0
                #s.user_run = self.org_service.get_run_user_count(start,end,**kargs)
                #s.new_user_run = self.org_service.get_new_runuser_count(start,end,**kargs)
                #s.user_visit = self.service.get_vis_user_count(start,end,**kargs)
                #s.new_user_visit = self.org_service.get_new_visuser_count(start,end,**kargs)
                #s.active_user_visit = self.service.get_active_user_count(start,end,**kargs)
                #if mode in ('week'):
                #    if scope['id'] == 1:
                #        counttype = 'ALL'
                #    if scope['id'] in (39,40,41,42):
                #        counttype = 'VERSION'
                #    else:
                #        counttype = ''
                #    s.user_retention = self.service.get_retention_user_count(mode,counttype,start,end,**kargs)
                #s.pay_user = self.down.get_pay_user_count(start,end,**kargs)
                #s.cpay_down = self.down.get_downcount(start,end,'chapter',ispay=True,**kargs)
                #s.cfree_down = self.down.get_downcount(start,end,'chapter',**kargs)
                #s.bpay_down = self.down.get_downcount(start,end,'book',ispay=True,**kargs)
                #s.bfree_down = self.down.get_downcount(start,end,'book',**kargs)
                #s.cpay_user = self.down.get_downcount(start,end,'chapter',ispay=True,is_user_unique=True,**kargs)
                #s.cfree_user = self.down.get_downcount(start,end,'chapter',is_user_unique=True,**kargs)
                #s.bpay_user = self.down.get_downcount(start,end,'book',ispay=True,is_user_unique=True,**kargs)
                #s.bfree_user = self.down.get_downcount(start,end,'book',is_user_unique=True,**kargs)
                batch = self.down.get_batch_basic(start, end, **kargs)
                #s.batch_pv = batch['batch_pv']
                #s.batch_uv = batch['batch_uv']
                s.batch_fee = batch['batch_fee']
                #if mode in ('week','month','year'):
                #    sum_byday = BasicStat.mgr().get_data(s.scope_id,'day',start,end)
                #    s.cfee,s.bfee = sum_byday['cfee'],sum_byday['bfee']
                #else:
                #    s.cfee = self.down.get_fee_by_chapter(start,end,**kargs)
                #    s.bfee = self.down.get_fee_by_book(start,end,**kargs)
                #s.recharge = self.down.get_recharge(start,end,**kargs)
                
                s.save()
        except Exception,e:
            s = None
            logging.error('%s\n',str(e),exc_info=True)
        return s

    def patch_book(self, mode, start, **kargs):
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return None
        start,end = self.normlize_time(mode,start)
        try:
            s = BookStat.mgr(ismaster=1).Q(time=start).filter(scope_id=scope['id'],mode=mode,time=start)
            batch = self.down.get_batch_basic(start, end, **kargs)
            debat = {'bid':0,'batch_pv':0,'batch_uv':0,'batch_fee':0}
            for book in s: 
                book_id = book.book_id
                down = batch.get(book_id, debat)
                book.batch_pv = down['batch_pv']
                book.batch_uv = down['batch_uv']
                book.batch_fee = down['batch_fee']
                book.save()
        except Exception,e:
            s = None
            logging.error('%s\n',str(e),exc_info=True)
        return s
  
    def import_visit_stat(self, mode, start, **kargs):
        '''
        mode: hour,day,week,month
        start: start time
        kargs: platform_id,run_id,plan_id,partner_id,version_name,product_name
        return: number of stat result for page types  
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return 0
        start,end = self.normlize_time(mode,start)
        import_num = 0
        for i in self.service.get_pagetype_count(start,end,**kargs):
            try:
                q = VisitStat.mgr(ismaster=1).Q(time=start)
                s = q.filter(scope_id=scope['id'],mode=mode,type=i['type'],time=start)[0]
                if s:
                    s.pv = i['pv']
                    s.uv = i['uv']
                    s.save()
                else:
                    s = VisitStat.new()
                    s.scope_id = scope['id']
                    s.mode = mode
                    s.time = start
                    s.type = i['type']
                    s.pv = i['pv']
                    s.uv = i['uv']
                    s.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_topN(self, mode, start, **kargs):
        '''
        mode: hour,day,week,month
        start: start time
        kargs: platform_id,run_id,plan_id,partner_id,version_name,product_name
        return: number of stat result of topN vis for page types
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return 0
        start,end = self.normlize_time(mode,start)
        import_num = 0
        for top in self.service.get_topN(start,end,**kargs):
            for i,elem in enumerate(top['list']):
                try:
                    stat = TopNStat.new()
                    stat.scope_id = scope['id']
                    stat.mode = mode
                    stat.time = start
                    stat.type = top['type']
                    stat.no = i
                    stat.value = elem['val']
                    stat.pv = elem['pv']
                    stat.uv = elem['uv']
                    stat.save()
                    import_num += 1
                except Exception,e:
                    logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def merge_book_stat(self, start, end, **kargs):
        '''
        merge book stat: pv uv pay_down free_down pay_user free_user fee 
        '''
        for dtype in ['book','chapter']:
            try:
                if dtype == 'book':
                    down_stat = {'pay':self.down.get_topN_book_bybook(start,end,ispay=True,**kargs),
                                 'free':self.down.get_topN_book_bybook(start,end,ispay=False,**kargs)}
                elif dtype == 'chapter':
                    down_stat = {'pay':self.down.get_topN_book_bychapter(start,end,ispay=True,**kargs),
                                 'free':self.down.get_topN_book_bychapter(start,end,ispay=False,**kargs),
                                 'batch_download':self.down.get_batch_download(start, end, **kargs)}
                vis_stat = self.service.get_briefvisit(start,end,**kargs)
                result = [] 
                deflt = {'down':0,'user':0,'fee':0,'pv':0,'uv':0,'real_fee':0}
                bids = set(down_stat['pay'].keys()) | set(down_stat['free'].keys())
                for bid in bids:
                    item = {}
                    item['bid'] = bid
                    item['pay_down'] = down_stat['pay'].get(bid,deflt)['down']
                    item['pay_user'] = down_stat['pay'].get(bid,deflt)['user']
                    item['fee'] = down_stat['pay'].get(bid,deflt)['fee']
                    item['real_fee'] = down_stat['pay'].get(bid,deflt)['real_fee']
                    item['free_down'] = down_stat['free'].get(bid,deflt)['down']
                    item['free_user'] = down_stat['free'].get(bid,deflt)['user']
                    item['pv'] = vis_stat.get(bid,deflt)['pv']
                    item['uv'] = vis_stat.get(bid,deflt)['uv']
                    if dtype == 'chapter':
                        debat = {'bid':0,'batch_pv':0,'batch_uv':0,'batch_fee':0}
                        item['batch_pv'] = down_stat['batch_download'].get(bid, debat)['batch_pv']
                        item['batch_uv'] = down_stat['batch_download'].get(bid, debat)['batch_uv']
                        item['batch_fee'] = down_stat['batch_download'].get(bid, debat)['batch_fee']
                    else:
                        item['batch_pv'] = 0
                        item['batch_uv'] = 0
                        item['batch_fee'] = 0
                    result.append(item)
                yield dtype,result
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        
    def import_book_stat(self, mode, start, num, **kargs):
        '''
        mode: hour,day,week,month
        start: start time
        kargs:platform_id,run_id,plan_id,partner_id,version_name,product_name
        return: number of book stat
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return 0
        import_num = 0
        start,end = self.normlize_time(mode,start)
        for dtype,books in self.merge_book_stat(start,end,**kargs):
            for i in books:
                try:
                    stat = BookStat.new()
                    stat.scope_id = scope['id']
                    stat.mode = mode
                    stat.time = start
                    stat.book_id = i['bid']
                    stat.charge_type = dtype
                    stat.pv = i['pv']
                    stat.uv = i['uv']
                    stat.pay_down = i['pay_down']
                    stat.free_down = i['free_down']
                    stat.pay_user = i['pay_user']
                    stat.free_user = i['free_user']
                    stat.fee = i['fee']
                    stat.real_fee = i['real_fee']
                    stat.batch_pv = i['batch_pv']
                    stat.batch_uv = i['batch_uv']
                    stat.batch_fee = i['batch_fee']
                    stat.save()
                    import_num += 1
                except Exception,e:
                    logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_bookrefer_stat(self, mode, start, pkey, **kargs):
        '''
        mode: hour,day,week,month
        start: start time
        kargs:platform_id,run_id,plan_id,partner_id,version_name,product_name
        return: number of book stat from diffrent pkey
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return 0
        import_num = 0
        start,end = self.normlize_time(mode,start)
        #result = self.service.get_topN_book(start,end,pkey,**kargs)
        result = self.service.get_briefvisit(start,end,pkey,**kargs)
        for bid in result:
            try:
                stat = BookReferStat.new()
                stat.scope_id = scope['id']
                stat.mode = mode
                stat.time = start
                stat.book_id = bid 
                #stat.p_key = pkey
                stat.p_key = '1S1'  #由于搜索log改了，refer跟着改
                stat.pv = result[bid]['pv']
                stat.uv = result[bid]['uv']
                if stat.uv >= 10:
                    stat.save()
                    import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def update_one_week_fee_and_new_user_run(self, mode, time):
        '''
        update one week fee and new user run
        '''
        if mode != 'day':
            return
        start,end = self.normlize_time(mode,time)
        last_week_start = end - datetime.timedelta(days=7)
        import_num = 0
        try:
            stat = Arpu7DaysArpuStat.mgr(ismaster=1).Q(time=last_week_start).filter(time=last_week_start)[0]
            if stat:
                #stat.one_week_fee = self.org_service.get_arpu_one_week_fee(start,end)
                stat.new_user_visit = self.org_service.get_arpu_one_day_new_user_run(start,end)
                stat.save()
                import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_one_week_fee_and_new_user_run(self, mode, time):
        '''
        mode: hour,day,week,month
        start: start time
        return: one day's user run's a week fee
        '''
        if mode != 'day':
            return
        start,end = self.normlize_time(mode,time)
        import_num = 0
        one_week_fee = self.org_service.get_arpu_one_week_fee(start,end) 
        new_user_visit = self.org_service.get_arpu_one_day_new_user_run(start,end)
        try:
            stat = Arpu7DaysArpuStat().new()
            stat.time = start - datetime.timedelta(days=6)
            stat.one_week_fee = one_week_fee
            stat.new_user_visit = new_user_visit
            stat.save()
            import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_all_week_fee(self, mode, time):
        if mode != 'day':
            return 
        start,end = self.normlize_time(mode,time)
        import_num = 0
        all_week_fee = self.org_service.get_all_one_week_fee(start,end)
        for i in all_week_fee:
            str = i.split('\t')
            try:
                stat = ArpuOneWeekFee().new()
                stat.one_week_fee = str[0] + str[1]
                stat.new_user_visit = 0
                stat.time = datetime.datetime.strptime(str[2],"%Y-%m-%d") 
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_all_week_new_user_visit(self, mode, time):
        if mode != 'day':
            return 
        start,end = self.normlize_time(mode,time)
        import_num = 0
        all_week_new_user_visit = self.org_service.get_all_one_week_new_user_visit(start,end) 
        print all_week_new_user_visit
        for i in all_week_new_user_visit:
            str = i.split('\t')
            try:
                stat = TemparpuOneWeekNewUserVisit().new()
                stat.new_user_visit = str[0]
                stat.time = datetime.datetime.strptime(str[1],"%Y-%m-%d")
                print stat
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_toptag_bybook(self, mode, start):
        '''
        mode: hour,day,week,month
        start: start time
        return: top tags by download user count of each book in it
        '''
        if mode != 'day':
            return
        start,end = self.normlize_time(mode,start)
        q = Scope.mgr().Q().extra("status in ('pas','nice') AND find_in_set('book',mask)")
        import_num = 0
        for scope in q:
            result = {}
            for i in BookStat.mgr().Q(time=start).filter(scope_id=scope.id,mode=mode,time=start):
                info = Service.inst().extern.get_book_info2(i.book_id)
                if info and 'tag' in info.keys():
                    for t in info['tag'].split(','):
                        t = t.lower().strip()
                        if t in result:
                            result[t]['pay_down'] += i.pay_down
                            result[t]['free_down'] += i.free_down
                            result[t]['pay_user'] += i.pay_user
                            result[t]['free_user'] += i.free_user
                            result[t]['pv'] += i.pv
                            result[t]['uv'] += i.uv
                            result[t]['fee'] += i.fee
                        else:
                            result[t] = {'pay_down':i.pay_down,'free_down':i.free_down,'pay_user':i.pay_user,
                                         'free_user':i.free_user,'pv':i.pv,'uv':i.uv,'fee':i.fee}
            for t in result:
                try:
                    item = result[t]
                    stat = BookTagStat.new()
                    stat.scope_id = scope['id']
                    stat.mode = mode
                    stat.time = start
                    stat.tag = t.encode('utf-8')
                    stat.pay_down = item['pay_down']
                    stat.free_down = item['free_down']
                    stat.pay_user = item['pay_user']
                    stat.free_user = item['free_user']
                    stat.pv = item['pv']
                    stat.uv = item['uv']
                    stat.fee = item['fee']
                    stat.save()
                    import_num += 1
                except Exception,e:
                    logging.error('%s\n',str(e),exc_info=True)
        return import_num
     
    def import_product_stat(self, mode, start, num, **kargs):
        '''
        mode: hour,day,week,month
        start: start time
        kargs: platform_id,run_id,plan_id,partner_id,version_name,product_name
        return: number of topN product stat
        '''
        scope = kargs if 'id' in kargs else self.get_scope(**kargs)
        if not scope:
            return 0
        start,end = self.normlize_time(mode,start)
        uv_total,rlist_u = self.service.get_topN_product(start,end,num=num,**kargs)
        bfee_total,rlist_b = self.down.get_topN_product(start,end,'book',num=num,**kargs)
        cfee_total,rlist_c = self.down.get_topN_product(start,end,'chapter',num=num,**kargs)
        all = rlist_u + rlist_b + rlist_c
        import_num = 0
        for i in all:
            try:
                stat = ProductStat.new()
                stat.scope_id = scope['id']
                stat.mode = mode
                stat.time = start
                stat.type = i['type'] # user book chapter
                stat.product_name = i['name']
                stat.count = i['uv']  if i['type']=='user' else i['fee']
                stat.ratio = i['ratio']
                stat.save()
                import_num += 1
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_factory_database(self, mode, start):
        print 'logstatV2 -> logstatv3'
        start,end = self.normlize_time(mode,start)
        res = BasicStat.mgr().get_one_day_partner_stat(mode,start)
        import_num = 0
        for i in res:
            s = BasicStatv3.new()
            try:
                s.scope_id = i['scope_id']
                s.mode = i['mode']
                s.time = i['time']
                s.visits = i['visits']
                s.imei = 0
                s.user_run = i['user_run']
                s.new_user_run = i['new_user_run']
                s.user_visit = i['user_visit']
                s.new_user_visit = i['new_user_visit']
                s.active_user_visit = i['active_user_visit']
                s.user_retention = i['user_retention']
                s.pay_user = i['pay_user']
                s.cpay_down = i['cpay_down']
                s.cfree_down = i['cfree_down']
                s.bpay_down = i['bpay_down']
                s.bfree_down = i['bfree_down']
                s.cpay_user = i['cpay_user']
                s.cfree_user = i['cfree_user']
                s.bpay_user = i['bpay_user']
                s.bfree_user = i['bfree_user']
                s.batch_pv = i['batch_pv']
                s.batch_uv = i['batch_uv']
                s.batch_fee = i['batch_fee']
                s.cfee = i['cfee']
                s.bfee = i['bfee']
                s.recharge = i['recharge']
                if s.visits or s.imei or s.user_run or s.user_visit or s.active_user_visit or s.cpay_down or s.cfree_down or s.bpay_down or s.bfree_down:
                    import_num += 1
                    s.save()
            except Exception,e:
                logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_factory_stat(self, mode, start):
        start,end = self.normlize_time(mode,start)
        res_dict = Service.inst().stat.get_all_factstat(mode,start,end)
        if res_dict == None:
            return 0
        import_num = 0
        try:
            for i in res_dict.keys():
                stat = FactorySumStat.new()
                stat.time = start
                stat.factory_id = i
                stat.visits = res_dict[i]['visits']
                stat.imei = res_dict[i]['imei']
                stat.user_run = res_dict[i]['user_run']
                stat.new_user_run = res_dict[i]['new_user_run']
                stat.user_visit = res_dict[i]['user_visit']
                stat.new_user_visit = res_dict[i]['new_user_visit']
                stat.active_user_visit = res_dict[i]['active_user_visit']
                stat.user_retention = res_dict[i]['user_retention']
                stat.pay_user = res_dict[i]['pay_user']
                stat.cpay_down = res_dict[i]['cpay_down']
                stat.cfree_down = res_dict[i]['cfree_down']
                stat.bpay_down = res_dict[i]['bpay_down']
                stat.bfree_down = res_dict[i]['bfree_down']
                stat.cpay_user = res_dict[i]['cpay_user']
                stat.cfree_user = res_dict[i]['cfree_user']
                stat.bpay_user = res_dict[i]['bpay_user']
                stat.bfree_user = res_dict[i]['bfree_user']
                stat.cfee = res_dict[i]['cfee']
                stat.bfee = res_dict[i]['bfee']
                stat.save()
                import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def import_wap_partner_stat(self,mode,start):
        start,end = self.normlize_time(mode,start)
        res_list = self.wap.get_wap_partner_stat(start,end)
        import_num = 0
        try:
            for res in res_list:
                arr = res.split('\t')
                stat = WapPartner.new()
                stat.time = start
                stat.partner_id = arr[2]
                stat.pay_user = arr[0]
                stat.fee = '%.2f' % float(arr[1])
                stat.save()
                import_num += 1
        except Exception,e:
            logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def update_zero_new_user_run_stat(self,start,mode,should_update_zero_new_user_run_partner_list):
        print "updating zero new user run..."
        start,end = self.normlize_time(mode,start)
        import_num = 0 
        for partner in should_update_zero_new_user_run_partner_list:
        #find scope_id 
            scope = Scope.mgr().Q().filter(partner_id=partner,product_name='',run_id=0,version_name='')[0]
            if scope:
                try: 
                    s = BasicStat.mgr(ismaster=1).Q(time=start).filter(scope_id=scope['id'],mode=mode,time=start)[0]
                    if s:
                        s.new_user_run = s.new_user_visit 
                        s.save()
                        import_num += 1
                except Exception,e:
                    logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def update_new_user_run_stat(self,start,mode):
        print "updating new user run..."
        start,end = self.normlize_time(mode,start)
        import_num = 0 

        # get parnter list
        partner = Partner.mgr().get_all_partner()
        partner = partner[:]
        partner.insert(0,{'partner_id':0})
        #partner = [{'partner_id':110010},{'partner_id':110011},{'partner_id':0}]

        # get scope_id and update
        for i in partner:
        # A partner matchs '110/d{3}' means it is wp7 or wp8 ,don's need update,and ios partner don't update ether
            pattern = re.compile(r'^11\d{4}$')
            match = pattern.match(str(i['partner_id']))
            if match:
                print i['partner_id'],' do not update'
                continue
            if i['partner_id'] in DO_NOT_UPDATE_NEW_USER_RUN_PARTNERID_LIST:
                print i['partner_id'],' do not update'
                continue

            # not wp7 wp8    
            scope = Scope.mgr().Q().filter(partner_id=i['partner_id'],product_name='',run_id=0,version_name='')[0]
            scope_update = Scope.mgr().get_scopes_by_parnter_id_for_update_new_user_run(i['partner_id']) 
            if i['partner_id'] == 0:
                scope_update = [{'id':39},{'id':40},{'id':41},{'id':42},{'id':151988},{'id':321972},{'id':330925},{'id':443939},{'id':661502},{'id':669196},{'id':749766},{'id':876486},{'id':985700},{'id':1186259}]
            if scope and scope_update: 
                sum_new_user_run = BasicStat.mgr().get_sum_new_user_run_by_scope_ids(scope_update,start,mode)
                if sum_new_user_run['new_user_run'] == None:
                    sum_new_user_run['new_user_run'] = 0 
                try:
                    s = BasicStat.mgr(ismaster=1).Q(time=start).filter(scope_id=scope['id'],mode=mode,time=start)[0]
                    if s:
                        s.new_user_run = int(sum_new_user_run['new_user_run'])
                        s.save()
                        import_num += 1
                except Exception,e:
                    s = None
                    logging.error('%s\n',str(e),exc_info=True)
        return import_num

    def start_importing(self, mode, now=None, stats=None, partner_ids=None):
        '''
        entry to analyze and import stat into mysql
        '''
        n0 = datetime.datetime.now()
        # clear cache
        self.down.reset_cache()
        self.service.reset_cache()
        # extra sql for partner_ids
        extra = None
        if partner_ids:
            extra = 'partner_id in (%s)' % partner_ids
            self.down.set_extra(extra)
            self.service.set_extra(extra)

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

        # import the partner and product to scope
        if not (stats or partner_ids):
            self.import_partner_productname(mode,start)
       
        # loop all the valid scopes
        q = Scope.mgr().Q().extra("status in ('pas','nice')")
        extra and q.extra(extra)
        for scope in q:
            mask = scope.mask.split(',')
            modes = scope.modes.split(',')
            if mode not in modes:
                print 'ignoring ',mode,scope.id,scope.modes
                continue

            print 'processing',scope.id,mask,modes
            
            # patch basic stat
            if 'basic' in mask and stats and 'patch_basic' in stats:
                self.patch_basic(mode,start,**scope)

            if 'book' in mask and stats and 'patch_book' in stats:
                self.patch_book(mode,start,**scope)
            
            # basic stat
            if 'basic' in mask and (not stats or 'basic' in stats):
                self.import_basic_stat(mode,start,**scope)
        
            # visit stat of page types
            if 'visit' in mask and (not stats or 'visit' in stats):
                self.import_visit_stat(mode,start,**scope)

            # topN of the page types
            if 'topn' in mask and (not stats or 'topn' in stats):
                self.import_topN(mode,start,**scope)

            # book stat
            if 'book' in mask and (not stats or 'book' in stats):
                self.import_book_stat(mode,start,num=0,**scope)
                self.import_bookrefer_stat(mode,start,['search_main','search_button','hot_search_word'],**scope)
            
            # machine stat
            if 'product' in mask and (not stats or 'product' in stats):
                self.import_product_stat(mode,start,num=9,**scope)

        # update new_user_run stat
        if not stats or 'update' in stats:
            self.update_new_user_run_stat(start,mode)
            self.update_zero_new_user_run_stat(start,mode,SHOULD_UPDATE_ZERO_NEW_USER_RUN_PARTNER_LIST)
 
        # logstatV2 -> logstatv3 
        if not stats or 'basicv3' in stats:
            self.import_factory_database(mode,start)
       
        # merge basic to factory sum
        if not stats or 'factory' in stats:
            self.import_factory_stat(mode,start)

        # merge tag according to book stat
        if not stats or 'tag' in stats:
            self.import_toptag_bybook(mode,start)

        print mode,start,'time:',datetime.datetime.now()-n0
        return True

if __name__ == '__main__':
    s = Stat2db(HiveConf)
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
    assert mode in ('day','week','month')
    stats = stats.split(',') if stats else None
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
 
