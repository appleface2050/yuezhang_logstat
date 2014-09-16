#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat
from model.factory import Factory,Partner,Proportion
from service import Service
from lib.excel import Excel

class FactShowHandler(BaseHandler):
    def index(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        if not start:
            start = tody - datetime.timedelta(days=7)
        else: 
            start = datetime.datetime.strptime(start,'%Y-%m-%d') 

        scope_id = 313969 #联想乐商店的scope_id
        partner_id = 108693 #联想乐商店的partner_id
        mode = 'day'
 
        basics = BasicStat.mgr().get_multi_days_in_multi_month_proportion_stat(scope_id,mode,start,tody,partner_id)
        basics = basics[:]
        fields = ['visits','user_run','new_user_run','user_visit','new_user_visit','active_user_visit','user_retention',
                'pay_user','cpay_down','cfree_down','bpay_down','bfree_down','cpay_user','cfree_user','bpay_user','bfree_user',
                'cfee','bfee','batch_pv','batch_uv','batch_fee']
        acc_stats = {}
        for field in fields:
            acc_stats[field] = 0
        acc_stats['time'] = '总和'
        acc_stats['factory_name'] = '乐商店CPA'
        for i in basics:
            i['time'] = i['time'].strftime('%Y-%m-%d')
            i['factory_name'] = '乐商店CPA'
            for field in fields:
                acc_stats[field] = acc_stats[field] + i[field]
        basics += [acc_stats]
        count = len(basics)
        self.render('data/factshow.html',
                        basics=basics,start=start.strftime('%Y-%m-%d'),date=yest.strftime('%Y-%m-%d'),page=page,psize=psize,count=count)

        


