#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from conf.settings import PAGE_TYPE,IOS_RUN_ID_LIST
from model.stat import Scope,BasicStatv3,VisitStat,TopNStat,BookStat,ProductStat,Partner
from model.run import Run
from service import Service
from lib.excel import Excel

class IOSBasicHandler(BaseHandler):
    def basic(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        
        tody = self.get_date()
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        run_list = [i for i in self.run_list() if i['run_id'] in IOS_RUN_ID_LIST]
        basics = []
        if run_id == 0: #不限
            if partner_id:
                scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                    partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
                if scope:
                    scope_id_list = str(scope['id'])
                    dft = dict([(i,0) for i in BasicStatv3._fields])
                    basic_y = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,yest)[0]
                    basic_l = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,last)[0]
                    basic_y,basic_l = basic_y or dft,basic_l or dft
                    basic_y['title'],basic_l['title'] = '昨日统计','前日统计'
                    basics = [basic_y,basic_l]
                    for basic in basics:
                        basic['cfee'] = long(basic['cfee'])
                        basic['bfee'] = long(basic['bfee'])
                        basic['batch_fee'] = long(basic['batch_fee'])
            else:
                scope = Scope.mgr().get_ios_scope_id_list()
                if scope:
                    scope_id_list = ','.join([str(i['id']) for i in scope]) 
                    dft = dict([(i,0) for i in BasicStatv3._fields])
                    basic_y = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,yest)[0]
                    basic_l = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,last)[0]
                    basic_y,basic_l = basic_y or dft,basic_l or dft
                    basic_y['title'],basic_l['title'] = '昨日统计','前日统计'
                    basics = [basic_y,basic_l]
                    for basic in basics:
                        basic['cfee'] = long(basic['cfee'])
                        basic['bfee'] = long(basic['bfee'])
                        basic['batch_fee'] = long(basic['batch_fee'])
        else:
            # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
            tody = self.get_date()
            yest = tody - datetime.timedelta(days=1)
            last = yest - datetime.timedelta(days=1)
            start = tody - datetime.timedelta(days=30)
            basics,topn_sch,topn_hw,b_books,c_books = [],[],[],[],[]
            visit_y = dict([(i,{'pv':0,'uv':0}) for i in PAGE_TYPE])
            visit_l = dict([(i,{'pv':0,'uv':0}) for i in PAGE_TYPE])
            if scope:
                # basic stat
                dft = dict([(i,0) for i in BasicStatv3._fields])
                basic_y = BasicStatv3.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)[0] 
                basic_l = BasicStatv3.mgr().Q(time=last).filter(scope_id=scope.id,mode='day',time=last)[0] 
                basic_m = BasicStatv3.mgr().get_data(scope.id,'day',start,tody,ismean=True)
                basic_p = BasicStatv3.mgr().get_peak(scope.id,'day',start,tody)
                basic_y,basic_l = basic_y or dft,basic_l or dft
                basic_y['title'],basic_l['title'] = '昨日统计','前日统计'
                basic_m['title'],basic_p['title'] = '每日平均','历史峰值'
                basics = [basic_y,basic_l,basic_m,basic_p]
                for basic in basics:
                    basic['cfee'] = long(basic['cfee'])
                    basic['bfee'] = long(basic['bfee'])
                    basic['batch_fee'] = long(basic['batch_fee'])
                # page visit
                for i in VisitStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=yest):
                    visit_y[i['type']] = i
                for i in VisitStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=last):
                    visit_l[i['type']] = i
                # topN search & hotword
                q = TopNStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=yest)
                topn_sch = q.filter(type='search').orderby('no')[:10]
                topn_hw = q.filter(type='hotword').orderby('no')[:10]
                # books of by-book & by-chapter
                q = BookStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)
                b_books = Service.inst().fill_book_info(q.filter(charge_type='book').orderby('fee','DESC')[:10])
                c_books = Service.inst().fill_book_info(q.filter(charge_type='chapter').orderby('fee','DESC')[:10])
        self.render('data/ios_basic.html',
                    run_id=run_id,
                    partner_id=partner_id,
                    run_list=run_list,date=tody.strftime('%Y-%m-%d'),
                    basics = basics
                    )
    
    def week(self):
        self.basic_chart('week')

    def basic_chart(self, mode):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        run_list = [i for i in self.run_list() if i['run_id'] in IOS_RUN_ID_LIST]
        
        if run_id == 0: #不限
            scope = Scope.mgr().get_ios_scope_id_list()
            tody = self.get_date(1) + datetime.timedelta(days=1)
            yest = tody - datetime.timedelta(days=1)
            days = [tody-datetime.timedelta(days=i) for i in range(7 if mode=='week' else 30,0,-1)]
            start = self.get_argument('start','')
            action = self.get_argument('action','')
            if start:
                start = datetime.datetime.strptime(start,'%Y-%m-%d')
            elif mode == 'week':
                start = tody - datetime.timedelta(days=7)
            else:
                start = tody - datetime.timedelta(days=30)
            delta = tody - start
            days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
            basics = []
            if scope:
                if partner_id:
                    scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                        partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
                    if not scope:
                        scope_id_list = []
                        basics = []
                    else:
                        scope_id_list = str(scope['id'])
                        for i in days:
                            dft = dict([(j,0) for j in BasicStatv3._fields])
                            basic = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,i)[0]
                            basic['title'] = i.strftime('%Y-%m-%d')
                            basics.append(basic)
                else:
                    scope_id_list = ','.join([str(i['id']) for i in scope]) 
                    for i in days:
                        dft = dict([(j,0) for j in BasicStatv3._fields])
                        basic = BasicStatv3.mgr().get_sum_data_by_multi_scope(scope_id_list,i)[0]
                        basic['title'] = i.strftime('%Y-%m-%d')
                        basics.append(basic)
                for basic in basics:
                    basic['cfee'] = long(basic['cfee'])
                    basic['bfee'] = long(basic['bfee'])
                    basic['batch_fee'] = long(basic['batch_fee'])

        else: 
            # scope
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
            tody = self.get_date(1) + datetime.timedelta(days=1)
            yest = tody - datetime.timedelta(days=1)
            days = [tody-datetime.timedelta(days=i) for i in range(7 if mode=='week' else 30,0,-1)]
            start = self.get_argument('start','')
            action = self.get_argument('action','')
            if start:
                start = datetime.datetime.strptime(start,'%Y-%m-%d')
            elif mode == 'week':
                start = tody - datetime.timedelta(days=7)
            else:
                start = tody - datetime.timedelta(days=30)
            delta = tody - start
            days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
            basics = []
            if scope:
                for i in days:
                    dft = dict([(j,0) for j in BasicStatv3._fields])
                    s = BasicStatv3.mgr().Q(time=i).filter(scope_id=scope.id,mode='day',time=i)[0] or dft
                    s['title'] = i.strftime('%Y-%m-%d')
                    basics.append(s)
                for basic in basics:
                    basic['cfee'] = long(basic['cfee'])
                    basic['bfee'] = long(basic['bfee'])
                    basic['batch_fee'] = long(basic['batch_fee'])
        x_axis = ['%s'%i.strftime('%m-%d') for i in days] 
        results = {}
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        for k in [i for i in BasicStatv3._fields if i not in excludes]:
            results[k] ={'title':BasicStatv3._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in basics])}
        if action == 'export':
            excel_title = [('time','时间'),('user_run','启动用户'),('new_user_run','新增启动用户'),
                    ('user_visit','访问用户'),('new_user_visit','新增访问用户'),('pay_user','付费用户'),
                    ('active_user_visit','活跃用户'),('visits','访问PV'),
                    ('cpay_down','章付费数'),('bpay_down','本付费数'),
                    ('cpay_user','章付费用户'),('bpay_user','本付费用户'),
                    ('cfree_down','章免费数'),('bfree_down','本免费数'),
                    ('cfree_user','章免费用户'),('bfree_user','本免费用户'),
                    ('cfee','章月饼消费'),('bfee','本月饼消费'),('batch_fee','批量订购阅饼消费'),('batch_pv','批量订购PV'),('batch_uv','批量订购UV')
                ]
            xls = Excel().generate(excel_title,basics,1) 
            filename = 'basic_%s_%s.xls' % (mode,yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/ios_basic_chart.html',
                platform_id=platform_id,run_id=run_id,plan_id=plan_id,partner_id=partner_id,
                version_name=version_name,product_name=product_name,date=yest.strftime('%Y-%m-%d'),
                run_list=run_list,mode=mode,x_axis=x_axis,
                basics=basics,results=results,start=start.strftime('%Y-%m-%d')
                )

