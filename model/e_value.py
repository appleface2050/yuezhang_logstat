#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel

class EValue(Model):
    '''
    e-value model
    在hive中，bid可能存在脏数据 eg;013
    E值分析-按章E值
    '''
    _db = 'logstatV2'
    _table = 'downv6_chptbook_evalu_v2'
    _pk = 'id'
    _fields = set(['id','usnum','cfee','cpay_user','bid','startpoint','cpay_user_percentage','ARPU','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`usnum` BIGINT NOT NULL DEFAULT '0'",
               "`cfee` DOUBLE NOT NULL DEFAULT '0'",
               "`cpay_user` BIGINT NOT NULL DEFAULT '0'",
               "`bid` INT NOT NULL DEFAULT '0'",
               "`startpoint` varchar(32) NOT NULL DEFAULT ''",
               "`cpay_user_percentage` DOUBLE NOT NULL DEFAULT '0'",
               "`ARPU` DOUBLE NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid_startpoint` (`bid`,`startpoint`)")
    _fielddesc = {'id':'ID','usnum':'用户数量','cfee':'按章月饼消费',
                'cpay_user':'按章付费用户','bid':'书id',
                'startpoint':'startpoint',
                'cpay_user_percentage':'按章付费用户百分比',
                'ARPU':'ARPU',
                'uptime':'更新时间'}

    def get_all_data(self, bookid_list=None, startpoint='书城'):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype).filter(startpoint=startpoint)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q

class EValueStartPointClassify(Model):
    '''
    e-value startpoint model
    select bid,startpoint,count(1) num from temp_downv6_1u1b_cfree_stpoint group by bid,startpoint
    '''
    _db = 'logstatV2'
    _table = 'evalue_startpoint_classify'
    _pk = 'id'
    _fields = set(['id','bid','startpoint','usernum','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`startpoint` varchar(32) NOT NULL DEFAULT ''",
               "`usernum` BIGINT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid_startpoint` (`bid`,`startpoint`)")
    _fielddesc = {'id':'ID','bid':'书id','startpoint':'startpoint','usernum':'用户数量','uptime':'更新时间'}
    
    def get_all_data(self):
        '''
        '''
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        return q

class EValueStartPointMinCid(Model):
    '''
    e-value startpoint min cid model
    select bid,startpoint,count(1)usernum from ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1 group by bid,startpoint
    '''
    _db = 'logstatV2'
    _table = 'evalue_startpoint_min_cid'
    _pk = 'id'
    _fields = set(['id','bid','startpoint','usernum','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`startpoint` INT NOT NULL DEFAULT '-1'",
               "`usernum` INT NOT NULL DEFAULT '0'", 
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid_startpoint` (`bid`,`startpoint`)")

    def get_top10_startpoint(self,book_id):
        ext = 'startpoint NOT IN (1,2,3,21,22,23)'
        qtype = 'SELECT bid,startpoint,usernum'
        q = self.Q(qtype=qtype).filter(bid=book_id).set_limit(0,10).orderby('usernum','DESC')
        q = q.extra(ext)
        return q

class EValueStartPointAll(Model):
    '''
    e-value startpoint model
    select bid,startpoint,count(1) num from temp_downv6_1u1b_cfree_stpoint group by bid,startpoint
    '''
    _db = 'logstatV2'
    _table = 'evalue_startpoint_all'
    _pk = 'id'
    _fields = set(['id','bid','shucheng','shujia','top1','top2','top3','top4','top5','top6','top7','top8','top9','top10','dabao','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`shucheng` BIGINT NOT NULL DEFAULT '0'",
               "`shujia` BIGINT NOT NULL DEFAULT '0'",
               "`dabao` BIGINT NOT NULL DEFAULT '0'",
               "`top1` varchar(32) NOT NULL DEFAULT ''",
               "`top2` varchar(32) NOT NULL DEFAULT ''",
               "`top3` varchar(32) NOT NULL DEFAULT ''",
               "`top4` varchar(32) NOT NULL DEFAULT ''",
               "`top5` varchar(32) NOT NULL DEFAULT ''",
               "`top6` varchar(32) NOT NULL DEFAULT ''",
               "`top7` varchar(32) NOT NULL DEFAULT ''",
               "`top8` varchar(32) NOT NULL DEFAULT ''",
               "`top9` varchar(32) NOT NULL DEFAULT ''",
               "`top10` varchar(32) NOT NULL DEFAULT ''",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid` (`bid`)")
    _fielddesc = {'id':'ID','bid':'书id','shucheng':'书城','shujia':'书架','dabao':'打包','top':'最多的开始章节','uptime':'更新时间'}

    def get_all_data(self, bookid_list=None):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q

class EValueCharegeByBook(Model):
    '''
    按本E值
    '''
    _db = 'week_ana'
    _table = 'book_full_sumfee'
    _fields = set(['book_id','fee','free_user','pay_user','uv','e_val','fee_1wk','free_user_1wk','pay_user_1wk',
      'uv_1wk','e_val_1wk','fee_1d','free_user_1d','pay_user_1d','uv_1d','e_val_1d','fee_4wk','free_user_4wk',
      'pay_user_4wk','uv_4wk','e_val_4wk'])
    _scheme = ("`book_id` int(11) NOT NULL DEFAULT '0'",
        "`fee` double DEFAULT NULL",
        "`free_user` decimal(32,0) DEFAULT NULL",
        "`pay_user` decimal(32,0) DEFAULT NULL",
        "`uv` decimal(32,0) DEFAULT NULL",
        "`e_val` decimal(9,2) DEFAULT NULL",
        "`fee_1wk` double DEFAULT NULL",
        "`free_user_1wk` decimal(32,0) DEFAULT NULL",
        "`pay_user_1wk` decimal(32,0) DEFAULT NULL",
        "`uv_1wk` decimal(32,0) DEFAULT NULL",
        "`e_val_1wk` decimal(9,2) DEFAULT NULL",
        "`fee_1d` double DEFAULT NULL",
        "`free_user_1d` decimal(32,0) DEFAULT NULL",
        "`pay_user_1d` decimal(32,0) DEFAULT NULL",
        "`uv_1d` decimal(32,0) DEFAULT NULL",
        "`e_val_1d` decimal(9,2) DEFAULT NULL",
        "`fee_4wk` double DEFAULT NULL",
        "`free_user_4wk` decimal(32,0) DEFAULT NULL",
        "`pay_user_4wk` decimal(32,0) DEFAULT NULL",
        "`uv_4wk` decimal(32,0) DEFAULT NULL",
        "`e_val_4wk` decimal(9,2) DEFAULT NULL",
        "KEY `idx_bid` (`book_id`)")
  
    def get_all_data(self,bookid_list=None):
        qtype = 'SELECT book_id,fee,free_user,pay_user,uv,e_val,e_val_1wk,e_val_4wk'
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("book_id in (%s)" % ','.join(bookid_list)) 
        return q        

class EValueMaxCidMaxFee(TemplateModel):
    '''
    e-value max cid max fee by book_id
    select bid,maxcid,sumprice from ana_tempdb.temp_downv6_chptbook_dim_maxcid_maxfee
    '''
    _table = 'evalue_maxcid_maxfee'
    _fields = set(['id','bid','maxcid','maxfee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`maxcid` BIGINT NOT NULL DEFAULT '0.0'",
               "`maxfee` FLOAT NOT NULL DEFAULT '0.0'", 
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid` (`bid`)")

    def get_all_data(self, bookid_list=None):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q

class EValuePayDistribute(TemplateModel):
    '''
    e-value pay distribute stat
    select * from ana_tempdb.temp_downv6_chptbook_p10_distrib
    '''
    _table = 'evalue_pay_distribute'
    _fields = set(['id','bid','p10','p20','p30','p40','p50','p60','p70','p80','p90',
                    'p10_usnum','p20_usnum','p30_usnum','p40_usnum','p50_usnum','p60_usnum','p70_usnum','p80_usnum','p90_usnum','p100_usnum','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`p10` DOUBLE NOT NULL DEFAULT '0'",
               "`p20` DOUBLE NOT NULL DEFAULT '0'",
               "`p30` DOUBLE NOT NULL DEFAULT '0'",
               "`p40` DOUBLE NOT NULL DEFAULT '0'",
               "`p50` DOUBLE NOT NULL DEFAULT '0'",
               "`p60` DOUBLE NOT NULL DEFAULT '0'",
               "`p70` DOUBLE NOT NULL DEFAULT '0'",
               "`p80` DOUBLE NOT NULL DEFAULT '0'",
               "`p90` DOUBLE NOT NULL DEFAULT '0'",
               "`p10_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p20_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p30_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p40_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p50_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p60_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p70_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p80_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p90_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`p100_usnum` BIGINT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid` (`bid`)")

    def get_all_data(self, bookid_list=None):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q

class EValueFreeDistributeMid(TemplateModel):
    '''
    e-value pay distribute stat middle
    select * from ana_tempdb.temp_downv6_chptbook_freechpt_distrib
    '''
    _table = 'evalue_free_distribute_mid'
    _fields = set(['id','bid','cidgroup','usernum','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`cidgroup` INT NOT NULL DEFAULT '0'",
               "`usernum` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid_cidgroup` (`bid`,`cidgroup`)")

    def get_all_data(self, bookid_list=None):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        #return q
        return self.convert_data(q)
    
    def convert_data(self,inlist):
        cache = {}
        for q in inlist:
            dft = {'-100':0, '1':0, '10':0, '20':0, '30':0}
            key = q['bid']
            if cache.get(key,None) is None:
                cache[key] = dft
            cache[key][str(q['cidgroup'])] = q['usernum']
        books = []
        for i in cache:
            book = {}
            book['bid'] = i
            book['stat'] = cache[i] 
            books.append(book)
        return books

class EValueFreeDistribute(TemplateModel):
    '''
    e-value pay distribute stat
    select * from ana_tempdb.temp_downv6_chptbook_freechpt_distrib
    '''
    _table = 'evalue_free_distribute'
    _fields = set(['id','bid','d_1','d_10','d_20','d_30','d_minue_100','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`bid` BIGINT NOT NULL DEFAULT '0'",
               "`d_1` INT NOT NULL DEFAULT '0'",
               "`d_10` INT NOT NULL DEFAULT '0'",
               "`d_20` INT NOT NULL DEFAULT '0'",
               "`d_30` INT NOT NULL DEFAULT '0'",
               "`d_minue_100` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bid` (`bid`)")

    def get_all_data(self, bookid_list=None):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        bookid_list and q.extra("bid in (%s)" % ','.join(bookid_list)) 
        return q


