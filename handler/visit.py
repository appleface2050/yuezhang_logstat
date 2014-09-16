#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from conf.settings import PAGE_TYPE
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat
from service import Service

class VisitHandler(BaseHandler):
    def index(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else: 
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date()
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        start = tody - datetime.timedelta(days=30)
        visit_y = dict([(i,{'pv':0,'uv':0}) for i in PAGE_TYPE])
        visit_l = dict([(i,{'pv':0,'uv':0}) for i in PAGE_TYPE])
        if scope:
            # page visit
            for i in VisitStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=yest):
                visit_y[i['type']] = i
            for i in VisitStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=last):
                visit_l[i['type']] = i
        self.render('data/visit.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=tody.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list = self.plan_list(), visit_y=visit_y,visit_l=visit_l,
                    )
    def week(self):
        self.visit_chart('week')

    def month(self):
        self.visit_chart('month')

    def visit_chart(self, mode):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else: 
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        days = [tody-datetime.timedelta(days=i) for i in range(7 if mode=='week' else 30,0,-1)]
        start = self.get_argument('start','')
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d') 
        elif mode == 'week':
            start = tody - datetime.timedelta(days=7)
        else: 
            start = tody - datetime.timedelta(days=30)
        delta = tody - start
        days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
        visits = []
        if scope:
            for day in days:
                visit = dict([(i,{'pv':0,'uv':0}) for i in PAGE_TYPE])
                for i in VisitStat.mgr().Q().filter(scope_id=scope.id,mode='day',time=day):
                    visit[i['type']] = i
                visit['title'] = day.strftime('%Y-%m-%d')
                visits.append(visit)
        x_axis = ['%s'%i.strftime('%m-%d') for i in days] 
        results = {}
        for v in visits:
            for t in v:
                if t == 'title':
                    continue
                results.setdefault(t,{'pv':[],'uv':[],'title':PAGE_TYPE[t][2]})
                for k in ['pv','uv']:
                    results[t][k].append(v[t][k])
        for t in results:
            for k in ['pv','uv']:
                results[t][k] = ','.join([str(i) for i in results[t][k]])
        self.render('data/visit_chart.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,partner_id=partner_id,
                    date=yest.strftime('%Y-%m-%d'),version_name=version_name,
                    product_name=product_name,run_list=self.run_list(),plan_list=self.plan_list(),
                    mode=mode,x_axis=x_axis,results=results,visits=visits,start=start.strftime('%Y-%m-%d')
                    )

