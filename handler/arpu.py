#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from conf.settings import PAGE_TYPE
from model.stat import Scope,BasicStat,ArpuOneWeekFee,Arpu7DaysArpuStat,Arpu30DaysArpuFeeStat,Arpu90DaysArpuFeeStat
from model.run import Run
from service import Service
from model.factory import Factory,Partner

class ArpuHandler(BaseHandler):
    def index(self):
        self.arpu_multi_days('week')

    def arpu_multi_days(self, mode):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
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
        
        # scope list
        all_fact_id = Factory.mgr().get_all_factory()
        scopeid_list = []
        for fact_id in all_fact_id:
            partner_list = Partner.mgr().Q().filter(factory_id=fact_id['id'])[:]
            for i in partner_list:
                scope2 = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id, 
                    partner_id=i.partner_id,version_name=version_name,product_name=product_name)[0]
                if scope2:
                    scopeid_list.append(scope2.id)
        
        if scope:
            for i in days:
                dft = dict([(j,0) for j in BasicStat._fields])
                s = BasicStat.mgr().Q(time=i).filter(scope_id=scope.id,mode='day',time=i)[0] or dft
                recharge = BasicStat.mgr().get_recharge_by_multi_scope_one_day(scopeid_list, i, mode='day')
                arpu_7days_stat = Arpu7DaysArpuStat.mgr().get_arpu_7days_stat(i)[0]
                arpu_30days_stat = Arpu30DaysArpuFeeStat.mgr().get_arpu_30_days_stat(i)[0]
                arpu_90days_stat = Arpu90DaysArpuFeeStat.mgr().get_arpu_90_days_stat(i)[0]
                s['title'] = i.strftime('%Y-%m-%d')
                s['recharge'] = recharge
                s['arpu_30days'],s['arpu_90days'] = 0,0
                # 7 days arpu and new_user_visit
                if arpu_7days_stat:
                    s['new_user_visit'] = arpu_7days_stat['new_user_visit']
                    if arpu_7days_stat['new_user_visit'] == 0:
                        s['arpu_7days'] = 0
                        s['one_week_fee'] = 0
                    else:
                        s['arpu_7days'] = '%.03f' % (arpu_7days_stat['one_week_fee']/arpu_7days_stat['new_user_visit'])
                        s['one_week_fee'] = arpu_7days_stat['one_week_fee']
                else:
                    s['new_user_visit'] = 0
                    s['arpu_7days'] = 0
                    s['one_week_fee'] = 0
                #30 days arpu
                if arpu_30days_stat:
                    if s['new_user_visit'] != 0:
                        s['arpu_30days'] = '%0.3f' % (arpu_30days_stat['thirty_days_fee']/s['new_user_visit'])
                else:
                    s['arpu_30days'] = 0
                #90 days arpu
                if arpu_90days_stat:
                    if s['new_user_visit'] != 0:
                        s['arpu_90days'] = '%0.3f' % (arpu_90days_stat['ninety_days_fee']/s['new_user_visit'])
                else:
                    s['arpu_90days'] = 0
                # arpu
                if s['cpay_user'] != 0:
                    s['arpu'] = '%.03f' % (s['recharge']/s['user_run'])
                else:
                    s['arpu'] = 0
                basics.append(s)
        x_axis = ['%s'%i.strftime('%m-%d') for i in days] 
        results = {}
        arpu_list = []
        excludes = ('id','scope_id','mode','time','recharge','uptime','bfree_down','visits','cfree_user','bfee','imei','user_retention','cfee','batch_uv',
            'pay_user','cfree_down','bpay_user','batch_pv','user_visit','batch_fee','active_user_visit','cpay_down','bfree_user','bpay_down')
        for k in [i for i in BasicStat._fields if i not in excludes]:
            results[k] ={'title':BasicStat._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in basics])}
        for basic in basics:
            arpu_list.append(basic['arpu'])
        arpu_data = ','.join(arpu_list)
        results['arpu'] = {'data':arpu_data,'title':'arpu'}
        self.render('data/arpu.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,partner_id=partner_id,
                    version_name=version_name,product_name=product_name,date=yest.strftime('%Y-%m-%d'),
                    run_list=self.run_list(),plan_list=self.plan_list(),mode=mode,x_axis=x_axis,
                    basics=basics,results=results,start=start.strftime('%Y-%m-%d')
                    )









