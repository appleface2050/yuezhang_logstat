#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from model.moniter import MoniterStat,MoniterV5Stat
from service import Service
from conf.settings import REPORT_V5_MONITOR_REVERSE,REPORT_V5_MONITOR

class MoniterHandler(BaseHandler):
    def index(self):

        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = yest
        end = tody
        version_name = self.get_argument('version_name','')
        start = self.get_argument('start','')
        end = self.get_argument('end','')
        version_name = self.get_argument('version_name','').replace('__','.')
        if not start:
            start = yest.strftime('%Y-%m-%d')
        if not end: 
            end = tody.strftime('%Y-%m-%d')
        q_start = MoniterStat.mgr().get_all_stat(time=start,version_name=version_name) 
        q_end = MoniterStat.mgr().get_all_stat(time=end,version_name=version_name) 
        result = {}
        result['start'] = q_start
        result['end'] = q_end
        x_axis = range(0,24)
        fields = ["visit","fee","run","arpu"]
        res_start, res_end = {}, {}
        for field in fields:
            res_start[field] = [int(i[field]) for i in q_start if field != 'arpu']
            res_end[field] = [int(i[field]) for i in q_end if field != 'arpu']
            if field == 'arpu':
                res_start[field] = [float(i[field]) for i in q_start]
                res_end[field] = [float(i[field]) for i in q_end]

        fielddesc = MoniterStat._fielddesc
        fielddesc_start, fielddesc_end = {}, {}
        for field in fielddesc:
            fielddesc_start[field] ='start-' + fielddesc[field]
            fielddesc_end[field] ='end-' + fielddesc[field]
        self.render('data/moniter.html',
                    version_name=version_name,start=start,end=end,result=result,x_axis=x_axis,res_start=res_start,res_end=res_end,
                    fields=fields,fielddesc_start=fielddesc_start,fielddesc_end=fielddesc_end)


    def moniterv5stat(self):
        plan_list = REPORT_V5_MONITOR_REVERSE.keys()

        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = yest
        end = tody
        scheme = self.get_argument('scheme','')
        if scheme:
            scheme = REPORT_V5_MONITOR_REVERSE[scheme]
        start = self.get_argument('start','')
        end = self.get_argument('end','')
        if not start:
            start = yest.strftime('%Y-%m-%d')
        if not end: 
            end = tody.strftime('%Y-%m-%d')
        q_start = MoniterV5Stat.mgr().get_moniter_v5_stat(time=start,scheme=scheme)
        q_end = MoniterV5Stat.mgr().get_moniter_v5_stat(time=end,scheme=scheme)

        result = {}
        result['start'] = q_start
        result['end'] = q_end
        x_axis = range(0,24)
        fields = ['download_pv','download_uv','pay_pv','pay_uv','fee']
        res_start, res_end = {}, {}
        for field in fields:
            res_start[field] = [int(i[field]) for i in q_start if field != 'fee']
            res_end[field] = [int(i[field]) for i in q_end if field != 'fee']
            if field == 'fee':
                res_start[field] = [float(i[field]) for i in q_start]
                res_end[field] = [float(i[field]) for i in q_end]

        fielddesc = MoniterV5Stat._fielddesc
        fielddesc_start, fielddesc_end = {}, {}
        for field in fielddesc:
            fielddesc_start[field] ='start-' + fielddesc[field]
            fielddesc_end[field] ='end-' + fielddesc[field]
        if scheme:
            scheme = REPORT_V5_MONITOR[scheme]
        self.render('data/moniterv5stat.html',plan_list=plan_list,
                    scheme=scheme,start=start,end=end,result=result,x_axis=x_axis,res_start=res_start,res_end=res_end,
                    fields=fields,fielddesc_start=fielddesc_start,fielddesc_end=fielddesc_end)
 







