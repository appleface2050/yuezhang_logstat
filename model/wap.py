#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel
from model.factory import Factory
from model.stat import Partner
from conf.settings import WAP_PAGE_TYPE,WAP_PAGE_TYPE_BASIC

class WapPartner(TemplateModel):
    '''
    wap partner model
    WAP数据
    '''
    _db = 'logstatV2'
    _table = 'wap_partner_stat'
    _fields = set(['id','time','partner_id','first_visits','visits','user_visit','pay_user','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`partner_id` INT NOT NULL DEFAULT '0'",
               "`first_visits` INT NOT NULL DEFAULT '0'",
               "`visits` INT NOT NULL DEFAULT '0'",
               "`pay_user` INT NOT NULL DEFAULT '0'",
               "`fee` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `time_partnerid` (`time`,`partner_id`)")

    def get_stat_by_factory_list(self, factory_list, time):
        stat = []
        for factory in factory_list:
            partner_list = Partner.mgr().Q().filter(factory_id=factory['id'])[:] 
            partner_id_list = [p['partner_id'] for p in partner_list]
            #print partner_id_list
            res = self.get_stat_by_partner_id_list(partner_id_list,time)
            res['factory_name'] = factory['name']
            res['partner_id_list'] = partner_id_list
            #print res
            stat.append(res)
        return stat

    def get_stat_by_partner_id_list(self, partner_id_list, time):
        res = {}
        str_partner_id_list = []
        if partner_id_list:
            partner_id_list = [str(partner_id) for partner_id in partner_id_list]
            str_partner_id_list = ','.join(partner_id_list)
            
            qtype = "SELECT sum(pay_user)payuser,sum(fee)fee"
            ext = "partner_id in (%s)" % str_partner_id_list
            q = self.Q(qtype=qtype,time=time).extra(ext)
            res = q[0]
        return res

class WapFactory(Model):
    '''
    wap factory model
    WAP数据
    '''
    _db = 'logstatV2'
    _table = 'v_wap_stat'
    _fields = set(['id','time','factory_id','factory_name','first_visits','visits','pay_user','fee','uptime'])
    
    def get_all_stat(self, time):
        excludes = ('id','uptime','factory_id')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time).filter(time=time)
        return q

    def get_facotry_stat_multi_days(self,start,end):
        excludes = ('id','uptime','factory_id')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "time>='%s' and time<='%s'"%(start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class WapVisitStat(TemplateModel):
    '''
    wap visit stat
    '''
    _db = 'logstatV2'
    _table = 'wap_visit_stat'
    _fields = set(['id','time','partner_id','wap_type','page_type','pv','uv','uptime'])

    def get_wap_stat(self, time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        page_type = ["'%s'" % i for i in WAP_PAGE_TYPE.keys()] 
        tmp = ','.join(page_type)
        ext = "page_type in (%s)" % tmp
        q = self.Q(qtype=qtype,time=time).extra(ext).filter(time=time)
        return q

    def get_wap_visit_all_stat(self, time):
        touch_stat = {}
        basic_stat = {}
        sql_touch = "SELECT page_type,SUM(pv)pv,SUM(uv)uv FROM wap_visit_stat WHERE TIME = '%s' AND wap_type = 'touch' GROUP BY page_type" % time
        sql_basic = "SELECT page_type,SUM(pv)pv,SUM(uv)uv FROM wap_visit_stat WHERE TIME = '%s' AND wap_type = 'basic' GROUP BY page_type" % time
        q_touch = WapVisitStat.mgr().raw(sql_touch)
        q_basic = WapVisitStat.mgr().raw(sql_basic)
        for type in WAP_PAGE_TYPE:
            touch_stat[type] = {'pv':0, 'uv':0}
            for q in q_touch:
                if type == q['page_type']:
                    touch_stat[type] = {'pv':q['pv'], 'uv':q['uv']}
        for type in WAP_PAGE_TYPE_BASIC:
            basic_stat[type] = {'pv':0, 'uv':0}
            for q in q_basic:
                if type == q['page_type']:
                    basic_stat[type] = {'pv':q['pv'], 'uv':q['uv']}
        return touch_stat, basic_stat





class WapBookStat(TemplateModel):
    '''
    wap basic stat
    '''
    _db = 'logstatV2'
    _table = 'wap_book_stat'
    _fields = set(['id','time','partner_id','wap_type','book_id','charge_type','fee','pay_down','pay_user','visit_uv','login_uv','visit','read_pv','uptime'])

    def get_wap_book_stat(self, time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time)
        q = q.filter(time=time)
        return q












