#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model.stat import Scope,BasicStat,Partnerv2,MonthlyBusinessStatDaily
from model.factory import Factory,Partner
from conf.settings import KEY_PRE

class StatCache(object):
    pre = lambda k:KEY_PRE+'_stat_'+k
    lfactstat = pre('all_factstat')
    pfactstat = pre('proportion_factstat')
    def __init__(self, service):
        self.cache = service.cache

    def get_all_factstat(self, mode, start, end):
        key = '%s_%s_%s_%s' % (self.lfactstat,mode,start,end)
        #res = self.cache.get(key)
        #if not res:
        factory_list = Factory.mgr().Q().data()
        res = {}
        for i in factory_list:
            partner_list = Partner.mgr().Q().filter(factory_id=i.id)[:]
            scopeid_list = []
            for p in partner_list:
                scope = Scope.mgr().Q().filter(platform_id=6,run_id='',plan_id='',
                        partner_id=p.partner_id,version_name='',product_name='')[0]
                if scope:
                    scopeid_list.append(scope.id)
            basic = BasicStat.mgr().get_data_by_multi_scope(scopeid_list,mode=mode,start=start,end=end)
            res[i.id] = basic
        #self.cache.set(key,res,1800)
        return res

    def get_all_factstat_proportion(self, mode, start, end):
        key = '%s_%s_%s_%s' % (self.pfactstat,mode,start,end)
        res = self.cache.get(key)
        if not res:
            factory_list = Factory.mgr().Q().data()
            res = {}
            for i in factory_list:
                partner_list = Partnerv2.mgr().Q().filter(factory_id=i.id)[:]
                scope_proportion_list = []
                for p in partner_list:
                    scope = Scope.mgr().Q().filter(platform_id=6,run_id='',plan_id='',
                        partner_id=p.partner_id,version_name='',product_name='')[0]
                    if scope:
                        temp = {'proportion':p['proportion'],'scope_id':scope.id,'partner_id':p['partner_id']}
                        scope_proportion_list.append(temp)
                #basic = BasicStat.mgr().get_data_by_multi_scope(scopeid_list,mode=mode,start=start,end=end)
                basic = BasicStat.mgr().get_data_by_multi_scope_proportion(scope_proportion_list,mode=mode,start=start,end=end)
                res[i.id] = basic
            self.cache.set(key,res,1800)
        return res
    
    def get_all_partner_fee_monthly(self, mode, start, end):
        res = {}
        partner_list = Partner.mgr().Q().data()
        for p in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=6,run_id='',plan_id='',
                partner_id=p.partner_id,version_name='',product_name='')[0]
            if scope:
                recharge = BasicStat.mgr().get_recharge_by_multi_scope([scope.id],mode=mode,start=start,end=end)
                fee = float(recharge['consumefee']) + float(recharge['msgfee'])*0.4
                res[p.partner_id] = fee
        return res

    def get_all_partner_fee_daily(self, mode, start, end):
        res = {}
        partner_list = Partner.mgr().Q().data()
        for p in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=6,run_id='',plan_id='',
                partner_id=p.partner_id,version_name='',product_name='')[0]
            if scope:
                recharge = BasicStat.mgr().get_recharge_by_multi_scope([scope.id],mode=mode,start=start,end=end)
                #fee = int(recharge['consumefee']) + int(recharge['msgfee'])
                fee = float(recharge['consumefee']) + recharge['msgfee']
                res[p.partner_id] = fee
        return res

       


