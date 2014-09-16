#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from conf.settings import TOP_TYPE
from model.stat import Scope,TopNStat
from service import Service

class TopNHandler(BaseHandler):
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
        last = yest - datetime.timedelta(days=1)
        start = tody - datetime.timedelta(days=30)
        count,results = 0,[]
        if scope:
            q = TopNStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest,type=top_type)
            count = q.count()
            results = q.orderby('no')[(page-1)*psize:page*psize]
            if top_type in ('board','subject'):
                if top_type == 'board':
                    type = 1
                else:
                    type = 2
                results = Service.inst().fill_section_info(results,type)
        
        # pagination
        page_count = (count+psize-1)/psize
        self.render('data/topn.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                    count=count, page_count=page_count, results=results, top_type=top_type, TOP_DICT=TOP_TYPE
                    )

