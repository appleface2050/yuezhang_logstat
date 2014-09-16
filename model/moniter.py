#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel
from conf.settings import REPORT_V5_MONITOR_REVERSE,REPORT_V5_MONITOR

class MoniterStat(Model):
    '''
    Moniter stat
    '''
    _db = 'dw_v6_test'
    _table = 'report_v6_monitoring_data'
    _fields = set(['ds','hour','version_name','run','visit','fee','rate','aurp'])
    _fielddesc = {'ds':'日期',
                  'hour':'小时',
                  'version_name':'版本',
                  'run':'启动用户',
                  'visit':'访问用户',
                  'fee':'收益',
                  'rate':'比例',
                  'arpu':'访问用户arpu',}

    def get_all_stat(self, time, version_name):
        
        excludes = ('aurp','rate')
        if not version_name:
            qtype = 'SELECT SUM(visit)visit,SUM(fee)fee,SUM(run)run,ds,version_name,hour'
            q = self.Q(qtype=qtype,time=time).filter(ds=time).groupby('hour')
        else:
            ext = "version_name = '%s'" % version_name
            sub = ','.join([i for i in self._fields if i not in excludes])
            qtype = 'SELECT %s' % sub
            q = self.Q(qtype=qtype,time=time).filter(ds=time).extra(ext)
        q = q.orderby('hour','ASC')
        for i in q:
            i['arpu'] = "%.2f" % (float(i['fee'])/float(i['visit']))
        result = self.fill_all_data(q, version_name, time)
        result = self.bubblesort(result)
        if not version_name:
            for i in result:
                i['version_name'] = '不限'
        return result

    def fill_data(self, res_list, version_name, time):
        hours = [i['hour'] for i in res_list] 
        max_hour = max(hours)
        res = res_list[:]
        while max_hour < 23:
            max_hour += 1
            dft = {'fee':'0', 'run':'0', 'hour':max_hour, 'version_name':version_name, 'visit':'0', 'ds':time, 'arpu':'0.0'}
            res.append(dft)
        return res

    def fill_all_data(self, res_list, version_name, time):
        hours = [i['hour'] for i in res_list]
        res = res_list[:]
        for hour in range(0,24):
            if hour not in hours:
                dft = {'fee':'0', 'run':'0', 'hour':hour, 'version_name':version_name, 'visit':'0', 'ds':time, 'arpu':'0.0'}
                res.append(dft)
        return res

    def bubblesort(self, dict_in_list):
        for j in range(len(dict_in_list)-1,-1,-1):
            for i in range(j):
                if dict_in_list[i]['hour'] > dict_in_list[i+1]['hour']:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list

class MoniterV5Stat(Model):
    '''
    Moniter v5 stat
    '''
    _db = 'dw_v6_test'
    _table = 'report_v5_monitoring_data'
    _fields = set(['ds','hour','scheme','download_pv','download_uv','pay_pv','pay_uv','fee'])
    _fielddesc = {'ds':'日期',
                  'hour':'小时',
                  'scheme':'方案',
                  'download_pv':'下载PV',
                  'download_uv':'下载UV',
                  'pay_pv':'付费PV',
                  'pay_uv':'付费UV',
                  'fee':'收益'}

    def get_moniter_v5_stat(self, time, scheme):
        excludes = ('time')
        if not scheme:
            qtype = 'SELECT SUM(download_pv)download_pv,SUM(download_uv)download_uv,SUM(pay_pv)pay_pv,SUM(pay_uv)pay_uv,SUM(fee)fee,ds,hour'
            q = self.Q(qtype=qtype,time=time).filter(ds=time).groupby('hour')
        else:
            ext = "scheme = '%s'" % scheme
            sub = ','.join([i for i in self._fields if i not in excludes])
            qtype = 'SELECT %s' % sub
            q = self.Q(qtype=qtype,time=time).filter(ds=time).extra(ext)
        q = q.orderby('hour','ASC')
        #for i in q:
        #    i['arpu'] = "%.2f" % (float(i['fee'])/float(i['visit']))
        result = self.fill_all_data(q, scheme, time)
        result = self.bubblesort(result)
        if not scheme:
            for i in result:
                i['scheme'] = '不限'
        for i in result:
            if i['scheme'] != '不限':
                i['scheme'] = REPORT_V5_MONITOR[i['scheme']]
        return result

    def fill_all_data(self, res_list, scheme, time):
        hours = [i['hour'] for i in res_list]
        res = res_list[:]
        for hour in range(0,24):
            if hour not in hours:
                dft = {'fee':'0', 'download_pv':'0', 'hour':hour, 'scheme':scheme, 'download_uv':'0', 'ds':time, 'pay_pv':'0', 'pay_uv':'0'}
                res.append(dft)
        return res

    def bubblesort(self, dict_in_list):
        for j in range(len(dict_in_list)-1,-1,-1):
            for i in range(j):
                if dict_in_list[i]['hour'] > dict_in_list[i+1]['hour']:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list









