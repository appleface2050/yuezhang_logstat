#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel
from model.factory import Factory
from model.stat import Partner
from conf.settings import WAP_PAGE_TYPE,WAP_PAGE_TYPE_BASIC


class PaymentRechargeDailyStat(Model):
    '''
    payment_recharge_daily_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_recharge_daily_stat'
    _fields = set(['id','time','partner_id','version_name','recharge_fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`version_name` varchar(32) NOT NULL DEFAULT ''",
        "`recharge_fee` float NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharge_daily_stat_partnerid_versionname` (`time`,`partner_id`,`version_name`)")

class PaymentPayDailyStat(Model):
    '''
    payment_pay_daily_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_pay_daily_stat'
    _fields = set(['id','time','partner_id','version_name','amount','gift_amount','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`version_name` varchar(32) NOT NULL DEFAULT ''",
        "`amount` float NOT NULL DEFAULT 0",
        "`gift_amount` float NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_pay_daily_stat_time_partnerid_versionname` (`time`,`partner_id`,`version_name`)")

class PaymentRechargingDetailStat(Model):
    '''
    payment_recharging_detail_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_recharging_detail_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','curbustype','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`curbustype` varchar(128) NOT NULL DEFAULT ''",
        "`recharging_type` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_stat_tm_or_pa_id_in_cu_rc` (`time`,`origin`,`partner_id`,`innerver`,`curbustype`,`recharging_type`)")

class PaymentRechargingDetailOrderStat(Model):
    '''
    payment_recharging_detail_order_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_recharging_detail_order_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`recharging_type` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_order_stat_tm_or_pa_id_in_rc` (`time`,`origin`,`partner_id`,`innerver`,`recharging_type`)")

class PaymentRechargingDetailFinishStat(Model):
    '''
    payment_recharging_detail_order_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_recharging_detail_finish_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','recharging_type','amount','gift_amount','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`recharging_type` int NOT NULL DEFAULT 0",
        "`amount` float NOT NULL DEFAULT 0",
        "`gift_amount` float NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_stat_tm_or_pa_id_in` (`time`,`origin`,`partner_id`,`innerver`,`recharging_type`)")

class PaymentRechargingAmountOrderStat(Model):
    '''
    payment_recharging_amount_order_stat
    '''
    _db = 'payment_v6'
    _table = 'payment_recharging_amount_order_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','amount','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`amount` float NOT NULL DEFAULT 0",
        "`recharging_type` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_amount_order_stat_ti_or_pa_in_am_re` (`time`,`origin`,`partner_id`,`innerver`,`amount`,`recharging_type`)")

class PaymentRechargingAmountFinishStat(Model):
    '''
    '''
    _db = 'payment_v6'
    _table = 'payment_recharging_amount_finish_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','amount','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`amount` float NOT NULL DEFAULT 0",
        "`recharging_type` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_amount_finish_stat_ti_or_pa_in_am_re` (`time`,`origin`,`partner_id`,`innerver`,`amount`,`recharging_type`)")

class PaymentUserRunStat(Model):
    '''
    '''
    _db = 'payment_v6'
    _table = 'payment_user_run'
    _fields = set(['id','time','user_run','partner_id','innerver','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`user_run` int NOT NULL DEFAULT 0",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_user_run_ti_par_inn` (`time`,`partner_id`,`innerver`)")

class PaymentBalanceStat(Model):
    '''
    payment_balance stat
    '''
    _db = 'payment_v6'
    _table = 'payment_banlance_stat'
    _fields = set(['id','time','recharge_balance','gift_balance','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`recharge_balance` bigint NOT NULL DEFAULT 0",
        "`gift_balance` bigint NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_time` (`time`)")







