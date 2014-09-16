#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel

class Recommendation(TemplateModel):
    '''
    TemplateModel model
    '''
    _db = 'logstatV2'
    _table = 'recommendation'
    _fields = set(['id','time','appid','type','cnt'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`appid` INT NOT NULL DEFAULT '0'",
               "`type` varchar(32) NOT NULL DEFAULT ''",
               "`cnt` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `id` (`id`)")

    def get_recommendation_stat(self,start,end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "time>='%s' and time<='%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext).orderby('time')
        return q

    def get_recommendation_one_day_stat(self,time,appid):
        time = time.strftime('%Y-%m-%d')
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "appid != 'unknown'"
        if appid != '':
            q = self.Q(qtype=qtype,time=time).extra(ext).filter(time=time,appid=appid)
            dt = self.Q(qtype='SELECT distinct(appid)',time=time).extra(ext).filter(time=time,appid=appid)
        else:
            q = self.Q(qtype=qtype,time=time).extra(ext).filter(time=time)
            dt = self.Q(qtype='SELECT distinct(appid)',time=time).extra(ext).filter(time=time)
        return q, dt



