#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import getopt
import datetime
import logging

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from analyze import HiveQuery
from model.factory import Factory
from model.stat import Partner
from model.wap import WapPartner,WapFactory

class WapQuery(HiveQuery):
    def __init__(self, host, port):
        super(WapQuery,self).__init__(host,port)
        self.cache = {}

    def reset_cache(self):
        self.cache = {}

    def get_wap_partner_stat(self, start, end):
        '''
        get wap partner stat
        start: start time
        end: end time
        '''
        sub = "((booktype='10' AND prcode in (24,30,22,26,36,27,20,38,32,21,11,13,12,14,17,16,18,31,23,28,29,35,34,33,2,7,6)) OR (booktype='20' AND price>0))"
        sub = self.merge_sql(sub,self.or_sql('partner_id',[108,109,110,111]))
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        sql = "SELECT COUNT(DISTINCT(%s))pay_user,sum(rechargingnum),partner_id FROM uc_down_recharge WHERE %s group by partner_id" % (self.wap_uid(),sub)
        self.client.execute(sql)
        res = self.client.fetchAll()
        return res

    def get_wap_factory_stat(self, start, end):
        '''
        get wap factory stat
        start: start time
        end: end time
        '''
        time = start
        stat = []
        q = Factory.mgr().Q()
        factory_list = q[:]
        if factory_list:
            for factory in factory_list:
                partner_list = Partner.mgr().Q().filter(factory_id=factory['id'])[:]
                partner_id_list = [p['partner_id'] for p in partner_list]         
                res = self.get_stat_by_partner_id_list(partner_id_list,time)
                if res:                     
                #针对factory表里有，但partner表里没有对应factory_id的情况 pass
                    res['factory_name'] = factory['name']
                    res['factory_id'] = factory['id']
                    res['partner_id_list'] = partner_id_list
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
            #q = self.Q(qtype=qtype,time=time).extra(ext)
            q = WapPartner.mgr().Q(qtype=qtype,time=time).extra(ext)
            res = q[0]
            #print res
        return res

if __name__ == '__main__':
    s =  WapQuery('192.168.0.150','10000')
    try:
        opts,args = getopt.getopt(sys.argv[1:],'',['start=','end=','version_name=','partner_id='])
    except getopt.GetoptError,e:
        logging.error('%s\n',str(e),exc_info=True)
        sys.exit(2)
    jobtype,start,end,version_name,partner_id = '',None,None,None,None
    for o, a in opts:
        if o == '--start':
            start = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--end':
            end = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--version_name':
            version_name = a
        if o == '--partner_id':
            partner_id = a
    now = datetime.datetime.now()
    end = now 
    start = now - datetime.timedelta(days=1)
    print start
    print end
    kargs = {'version_name':version_name,'partner_id':partner_id}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
#    kargs = {'scope_id':82694,'mode':'week'}
    print '厂商数据',s.get_wap_partner_stat(start,end)
    print datetime.datetime.now() - now



