#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from handler.base import BaseHandler
from lib.excel import Excel
from model.wap import WapPartner,WapFactory,WapVisitStat,WapBookStat
from model.stat import Partner
from model.factory import Factory
from conf.settings import WAP_PAGE_TYPE
from service import Service

class WapHandler(BaseHandler):
    def index(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        top_type = self.get_argument('top_type')
        query_mode = self.get_argument('query_mode','') 
   
    def factorystat_bak(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        group = self.get_argument('group','') 
        factory_input_name = self.get_argument('factory_name','')

        #factory_name
        q = Factory.mgr().Q()
        factory_list = self.filter_wap_factory(q[:])
        factory_name_list = [str(i['name']) for i in factory_list]
        if factory_input_name != '':
            if factory_input_name in factory_name_list:
                factory_name = factory_input_name
            else:
                factory_name = None
        else:
            if len(factory_list) == 1:
                factory_name = factory_list[0]['name']
            else:
                factory_name = ''

        #datatime
        start = self.get_argument('start','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest 
        
        count,stats = 0,[]
        stats = WapFactory.mgr().get_facotry_stat_multi_days(start,end)
        if factory_name != '':
            stats = stats.filter(factory_name=factory_name)
        count = len(stats) 
        stats = stats[(page-1)*psize:page*psize]
        for stat in stats:
            stat['fee'] = "%.02f" % stat['fee']
            stat['time'] = stat['time'].strftime('%Y-%m-%d')
        self.render('data/wapfactory.html',
            factory_name=factory_name,start=start.strftime('%Y-%m-%d'),date=end.strftime('%Y-%m-%d'),
            page=page,psize=psize,count=count,stats=stats)

    def wapvisitallstat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = yest.strftime('%Y-%m-%d')
        count,stats = 0,[]
 
        touch_stat, basic_stat = WapVisitStat.mgr().get_wap_visit_all_stat(time=start)
        self.render('data/wap_visit_all_stat.html',
            date=start,touch_stat=touch_stat,basic_stat=basic_stat)

    def wapvisitstat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        #order_field = self.get_argument('orderField','pay_user')
        #assert order_field in ('first_visits','visits','pay_user','fee')
        
        partner_id = int(self.get_argument('partner_id',0))
        wap_type = self.get_argument('wap_type','')
        #page_type = self.get_argument('page_type','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = yest.strftime('%Y-%m-%d')
        count,stats = 0,[]
        
        stats = WapVisitStat.mgr().get_wap_stat(time=start)
        if wap_type:
            stats = stats.filter(wap_type=wap_type)
        if partner_id != 0:
            stats = stats.filter(partner_id=partner_id)
        count = len(stats)
        for stat in stats:
            stat['page_type'] = WAP_PAGE_TYPE[stat['page_type']]
        stats = stats[(page-1)*psize:page*psize] 

        self.render('data/wap_visit_stat.html',
            date=start,partner_id=partner_id,wap_type=wap_type,
            page=page,psize=psize,count=count,stats=stats)

    def wapbookstat(self):
        partner_id = int(self.get_argument('partner_id',0))
        wap_type = self.get_argument('wap_type','')
        charge_type = self.get_argument('charge_type','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        action = self.get_argument('action','')
        start = yest.strftime('%Y-%m-%d')
        count,stats = 0,[]
        stats = WapBookStat.mgr().get_wap_book_stat(time=start)

        if wap_type:
            stats = stats.filter(wap_type=wap_type)
        if partner_id != 0:
            stats = stats.filter(partner_id=partner_id)
        if charge_type:
            if charge_type == 'book':
                charge_type = '10'
            elif charge_type == 'chapter':
                charge_type = '20'
            stats = stats.filter(charge_type=charge_type)
        
        stats = Service.inst().fill_book_info(stats)
        count = len(stats)
        if action == 'export':
            title = [('book_id','书ID'),('name','书名'),('author','作者'),('cp','版权'),('category_0','类别'),('category_1','子类'),('wap_type','wap类型'),
                    ('charge_type','计费类型'),('state','状态'),('fee','月饼消费'),('pay_down','付费订购数'),('pay_user','付费用户数'),
                    ('visit_uv','访问UV'),('login_uv','登陆UV'),('visit','访问PV'),('read_pv','阅读PV')]
            xls = Excel().generate(title,stats,1)
            filename = 'book_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/wap_book_stat.html',wap_type=wap_type,charge_type=charge_type,
            page=page,psize=psize,date=start,stats=stats,partner_id=partner_id,count=count)

    def wapbasicstat(self):
        pass

    def factorystat(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        group = self.get_argument('group','') 
        factory_input_name = self.get_argument('factory_name','')

        #factory_name
        q = Factory.mgr().Q()
        factory_list = self.filter_wap_factory(q[:])
        factory_name_list = [str(i['name']) for i in factory_list]
        if factory_input_name != '':
            if factory_input_name in factory_name_list:
                factory_name = factory_input_name
            else:
                factory_name = None
        else:
            if len(factory_list) == 1:
                factory_name = factory_list[0]['name']
            else:
                factory_name = ''

        #datatime
        start = self.get_argument('start','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest 
        
        count,stats = 0,[]
        stats = WapFactory.mgr().get_facotry_stat_multi_days(start,end)
        if factory_name != '':
            stats = stats.filter(factory_name=factory_name)[:]
            if not stats:
                acc = {'first_visits': 0, 'fee': 0.0, 'factory_name': '', 'time': '总和', 'pay_user': 0, 'visits': 0}
            else:
                acc = {'first_visits': 0, 'fee': 0.0, 'factory_name': stats[0]['factory_name'], 'time': '总和', 'pay_user': 0, 'visits': 0}
                for stat in stats:
                    acc['first_visits'] += stat['first_visits']
                    acc['fee'] += stat['fee']
                    acc['pay_user'] += stat['pay_user']
                    acc['visits'] += stat['visits']
            stats.append(acc)

        count = len(stats) 
        stats = stats[(page-1)*psize:page*psize]
        for stat in stats:
            stat['fee'] = "%.02f" % stat['fee']
            if stat['time'] != '总和':
                stat['time'] = stat['time'].strftime('%Y-%m-%d')
        self.render('data/wapfactory.html',
            factory_name=factory_name,start=start.strftime('%Y-%m-%d'),date=end.strftime('%Y-%m-%d'),
            page=page,psize=psize,count=count,stats=stats)

       





