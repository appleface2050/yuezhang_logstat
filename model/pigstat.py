#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel

class A0(TemplateModel):
    '''
    A0
    '''
    _db = 'logstatV2'
    _table = 'pigstat_a0'
    _fields = set(['id','time','a0','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`a0` varchar(64) NOT NULL DEFAULT ''",
               "`pv` INT NOT NULL DEFAULT '0'",
               "`uv` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bookid` (`id`)")

    def get_pv_uv_from_a0(self,a0,time):
        qtype = "SELECT pv,uv"
        ext = "time = '%s' and a0= '%s'" % (time,a0)
        q = self.Q(qtype=qtype,time=time).extra(ext)[0]
        if q:
            pv = q['pv']
            uv = q['uv']
        else:
            pv, uv = 0, 0
        return pv,uv

    def get_pv_uv_multi_days(self,start,end):
        qtype = "SELECT *"
        ext = "time >= '%s' and time <= '%s'" % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'))
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q



