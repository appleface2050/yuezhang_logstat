#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: factory & partner Model

import os
import sys
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.database import Model

class Factory(Model):
    '''
    factory model,one factory has several partner_ids
    '''
    _db = 'logstatV2'
    _table = 'factory'
    _pk = 'id'
    _fields = set(['id','name','group','phone','email','intro','status','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`group` varchar(32) NOT NULL DEFAULT ''",
               "`phone` varchar(16) NOT NULL DEFAULT ''",
               "`email` varchar(64) NOT NULL DEFAULT ''",
               "`intro` varchar(255) NOT NULL DEFAULT ''",
               "`status` enum('hide','pas','nice') NOT NULL DEFAULT 'pas'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `name` (`name`)")
    def all_groups(self):
        qtype = 'SELECT DISTINCT(`group`) AS `group`'
        return [i.group for i in self.Q(qtype=qtype) if i.group]

    def get_all_factory(self):
        res = {}
        qtype = 'SELECT id'
        q = self.Q(qtype=qtype)
        res = q
        return res

class Proportion(Model):
    '''
    proportion model, one partner may have a 
    '''
    _db = 'logstatV2'
    _table = 'proportion'
    _pk = 'id'
    _fields = set(['id','proportion_id','partner_id','proportion','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`proportion_id` INT NOT NULL DEFAULT '0'",
               "`partner_id` INT NOT NULL DEFAULT '0'",
               "`proportion` FLOAT NOT NULL DEFAULT '1.0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `id_proportionid` (`id`,`proportion_id`)")

class Partner(Model):
    '''
    parnter model, each has a unique partner_id 
    '''
    _db = 'logstatV2'
    _table = 'partner'
    _pk = 'id'
    _fields = set(['id','partner_id','factory_id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`partner_id` INT NOT NULL DEFAULT '0'",
               "`factory_id` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `partner_id` (`partner_id`)")
    _extfds = set(['factory'])

class FactorySumStat(Model):
    '''
    factory sum model,
    '''
    _db = 'logstat'
    _table = 'factory_sum_stat'
    _pk = 'id'
    _fields = set(['id'])


if __name__ == "__main__":
#    f = Factory.new()
#    f.init_table()
#    p = Partner.new()
#    p.init_table()
    p = Proportion.new()
    p.init_table()
