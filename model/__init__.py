#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model

class TemplateModel(Model):
    '''
    a Template of Model 
    '''
    _db = 'logstatV2'
    _table = ''
    _pk = 'id'
    _fields = set(['id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)")
    _fielddesc = {'id':'ID','uptime':'更新时间'}
    
    def get_all_data(self):
        '''
        '''
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        return q

