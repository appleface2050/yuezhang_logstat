#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel

class AnaChapterDownload1u1bCount(Model):
    '''
    e-value model
    在hive中，bid可能存在脏数据 eg;013
    E值分析-按章E值
    '''
    _db = 'logstat'
    _table = 'ana_chapter_download_1u1b_count'
    _pk = 'id'
    _fields = set(['id','time','count','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` varchar(32) NOT NULL DEFAULT ''",
               "`count` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `time` (`time`)")
    _fielddesc = {'id':'ID',
                'time':'日期',
                'count':'数量',
                'ARPU':'ARPU',
                'uptime':'更新时间'}

    def get_all_data(self, bookid_list=None, startpoint='书城'):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype).filter(startpoint=startpoint)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q


