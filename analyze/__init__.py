#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import urllib2

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.hive import HiveClient
from lib.localcache import mem_cache
from conf.settings import API_TRY_TIME_LIMIT,ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML
from tools.send_email import SendEmail 
from model.stat import SyncThrdPartnerbyplan

class HiveQuery(object):
    sendemail = SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML)
    def __init__(self, host, port):
        self.client = HiveClient(host,port)
        self.extra_sql = None
        self.sendemail = SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML)

    def get_try_times(self):
        return self._try_times

    def zero_try_times(self):
        self._try_times = 0
        return True

    def acc_try_times(self):
        self._try_times += 1
        return True

    def get_send_email(self):
        return self.sendemail

    def set_extra(self, sub):
        '''
        extra query condition for all
        '''
        self.extra_sql = sub

    def execute(self, sql):
        '''
        proxy for self.client.execute
        '''
        if self.extra_sql:
            idx = sql.upper().find('WHERE')
            if idx >= 0:
                sql = '%s %s AND %s' % (sql[:idx+5],self.extra_sql,sql[idx+5:])
            else:
                sql = '%s WHERE %s' % (sql,self.extra_sql)
        return self.client.execute(sql)

    def or_sql(self, field, valist):
        return '(%s)' % ' OR '.join(["%s like '%s%%'"%(field,i) for i in valist])

    def in_sql(self, field, valist):
        vals = ','.join(["'%s'"%i for i in valist])
        return '%s in (%s)'%(field,vals)
    
    def merge_sql(self, where, cond):
        """
        merge subsqls
        """
        return "%s AND %s"%(where,cond) if where and cond else (where or cond)
    
    def group(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            group.append('run_id')
        if plan_id or partner_id:
            group.append('partner_id')
        if version_name:
            group.append("regexp_extract(version_name,'(ireader_[0-9].[0-9])')")
        if product_name:
            group.append('product_name')
        return ','.join(group)

    def regexp_firstinittime(self, firstinittime):
        time = firstinittime.strftime('%Y-%m-%d')
        return "firstinittime regexp '^%s.*$'" % time

    def regexp_fitime(self, fitime):
        time = fitime.strftime('%Y-%m-%d')
        return "fitime regexp '^%s.*$'" % time

    def regexp_afirstvisittime(self, firstvisittime):
        time = firstvisittime.strftime('%Y-%m-%d')
        return "a.firstvisittime regexp '^%s.*$'" % time

    def group_userset_init_pool(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            group.append('last_runid')
        if plan_id or partner_id:
            group.append('last_channel')
        if version_name:
            group.append("regexp_extract(last_ireaderver,'(ireader_[0-9].[0-9])')")
        if product_name:
            group.append('last_model')
        return ','.join(group)
    
    def group_basic_service_v6(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            group.append('runid')
        if plan_id or partner_id:
            group.append('partnerid')
        if version_name:
            #group.append("regexp_extract(versionname,'(ireader_[0-9].[0-9])')")
            #group.append("regexp_extract(versionname,'(ireader_[v|V]{0,1}[0-9]+\\\\.[0-9]+)')")
            group.append("CONCAT((regexp_extract(versionname,'.*?(ireader_)([v|V]{0,1})(\\\\d+\\\\.+\\\\d).*?',1)),(regexp_extract(versionname,'.*?(ireader_[v|V]{0,1})(\\\\d+\\\\.+\\\\d).*?',2))) ")
        if product_name:
            group.append('productname')
        return ','.join(group)

    def group_keys(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id','')
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        if plan_id:
            #partnerid_list = self.get_partnerid_list(plan_id)
            partnerid_list = self.get_partnerid_listV2(plan_id)
        else:
            partnerid_list = [partner_id]
        keys = []
        products = [product_name] if product_name!='unknown' else ['','null']
        for i in partnerid_list:
            for p in products:
                key = []
                if run_id:
                    key.append(str(run_id))
                if i:
                    key.append(str(i))
                if version_name:
                    key.append(version_name)
                if p or product_name=='unknown':
                    key.append(p)
                keys.append(','.join(key))
        return keys
 
    def grp(self, group):
        return ',%s'%group if group else ''

    def uid(self):
        return 'CONCAT(uid,user_name,reg_type)'
   
    def wap_uid(self):
        return 'CONCAT(ucid,loginname,logintype)'

    def uc_userId(self):
        return 'CONCAT(bk,uc_userId)'

    def ds_sql(self, start, end):
        s,e = start.strftime('%Y%m%d'),end.strftime('%Y%m%d')
        return "ds>='%s' AND ds<'%s'"%(s,e)

    def ds_sql_basic_service_v6(self, start, end):
        s,e = start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')
        return "ds>='%s' AND ds<'%s'"%(s,e)

    def ds_sql_userset_init_pool(self, start, end):
        s,e = start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')
        return "ds>='%s' AND ds<'%s'"%(s,e)

    def date_unix_stamp_sql(self, start, end, col='DATE'):
        if col == 'DATE':
            return "DATE>=unix_timestamp('%s') AND DATE<unix_timestamp('%s')" % (start,end)
        else:
            return "%s>=unix_timestamp('%s') AND %s<unix_timestamp('%s')" % (col,start,col,end)

    @classmethod
    def get_partnerid_list(cls, plan_id):
        '''
        get partner id list by plan id, to do
        http://192.168.0.7:10080/scheme?plan_id={plan_id}
        '''
        MySQLData._try_times = 0
        while(True):
            try:
                jsonstr = urllib2.urlopen('http://192.168.0.7:10080/scheme?plan_id=%s'%plan_id).read()
                break
            except Exception, e:
                print e
                MySQLData._try_times += 1
                time.sleep(30)
                if MySQLData._try_times >= API_TRY_TIME_LIMIT:
                    MySQLData.sendemail.send_mail()
                    return False
        return json.loads(jsonstr)['partner_id']

    def get_partnerid_listV2(self, plan_id):
        reslist = []
        entries = SyncThrdPartnerbyplan().mgr().get_partner_list_by_plan_id(plan_id)
        for entry in entries:
            partner_id = entry["partner_id"]
            reslist.append(str(partner_id))
        return reslist

    @classmethod
    def get_plan_list(cls):
        '''
        http://192.168.0.7:10080/scheme?platform_id=6   
        '''
        return [35, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]

    def group_userset_init(self, **kargs):                                                          
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            group.append('runid')
        if plan_id or partner_id:
            group.append('channel')
        if version_name:
            group.append("regexp_extract(ireaderver,'(ireader_[0-9].[0-9])')")
        if product_name:
            group.append('model')
        return ','.join(group)

    def group_userfirstinit(self, **kargs):                                                          
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            group.append('first_runid')
        if plan_id or partner_id:
            group.append('first_channel')
        if version_name:
            group.append("regexp_extract(first_ireaderver,'(ireader_[0-9].[0-9])')")
        if product_name:
            group.append('first_model')
        return ','.join(group)

    def ds_sql_userset_init(self, start, end):                                                                                                                          
        s,e = start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')
        return "ds>='%s' AND ds<'%s'"%(s,e)
