#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import getopt
import datetime
import logging

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from conf.settings import PAGE_TYPE,TOP_TYPE
from analyze import HiveQuery

class ServiceQuery(HiveQuery):
    '''
    query from service_v6
    '''
    def __init__(self, host, port):
        '''
        init hive client
        '''
        super(ServiceQuery,self).__init__(host,port)
        self.cache = {}
    
    def reset_cache(self):
        self.cache = {}

    def get_partner_product(self, start, end):
        sub = self.ds_sql_basic_service_v6(start,end)
        sql = "select partnerid,productname from basic_service_v6 where %s group by partnerid,productname"%sub
        self.execute(sql)
        res = {}
        for i in self.client.fetchAll():
            a = i.split('\t')
            key = a[0].strip()
            val = a[1].strip()
            if key.isdigit() and val:
                res.setdefault(key,['unknown']).append(val)
        return res

    def get_vis_user_count(self, start, end, **kargs):
        '''
        get count of access user
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.or_sql('partner_id',[108,109,110,111])
        sub = self.merge_sql(sub,self.in_sql('key',['4B4','4B104','1U1','1R1','1P1','1K1','1S1','17B','126B','128B','129B']))
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(%s)) %s FROM service_v6 WHERE %s %s"%(self.uid(),self.grp(group),sub,groupby)
        ckey = 'vis_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_retention_user_count(self, mode, counttype, start, end, **kargs):
        '''
        get count of users who visited first last week and visits this week still
        '''
        #print 'kargs_retent',kargs
        scope_id = kargs['id']
        count = 0
        print 'log id='+ str(scope_id)
        if mode != 'week' or counttype == '':
            return count
        if counttype == 'ALL':
            s,e = start.strftime('%Y%m%d'),end.strftime('%Y%m%d') 
            sub = 'a.ds>= %s and a.ds< %s and b.first_vis_time>= \'%s\' and b.first_vis_time< \'%s\' '%(s,e,start,end)
            sql = 'SELECT count(distinct a.uid) num FROM user_vis_byday a JOIN userbasic_v6_new b ON a.uid=b.uid WHERE %s'%(sub)
            self.execute(sql)
            count = self.client.fetchOne()
            return count
        
        if counttype == 'VERSION':
            s,e = start.strftime('%Y%m%d'),end.strftime('%Y%m%d') 
            sub = 'a.ds>= %s and a.ds< %s and b.first_vis_time>= \'%s\' and b.first_vis_time< \'%s\' '%(s,e,start,end)
            qtype = 'SELECT count(distinct a.uid) num,regexp_extract(b.version_name,\'(ireader_[0-9].[0-9])\') vername'
            group = ' regexp_extract(b.version_name,\'(ireader_[0-9].[0-9])\') having num>100'
            sql = '%s FROM user_vis_byday a JOIN userbasic_v6_new b ON a.uid=b.uid WHERE %s GROUP BY %s'%(qtype,sub,group)
            ckey = 'retention_user_%s_%s_%s'%(start,end,'version_name')
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                self.execute(sql)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,val = ','.join(arr[1:]),int(arr[0])
                    res[key] = val
                self.cache[ckey] = res
            count = res.get(scope_id,0)
            return count

    #def get_run_user_count(self, start, end, **kargs):
        #sub = self.or_sql('partner_id',[108,109,110,111])
        #sub = self.merge_sql(sub,self.ds_sql(start,end))
        #group = self.group(**kargs)
        #groupby = ('GROUP BY %s' % group) if group else ''
        #sql = "SELECT COUNT(DISTINCT(uid))%s FROM user_init_byday WHERE %s %s"%(self.grp(group),sub,groupby)
        #ckey = 'run_ucnt_%s_%s_%s'%(start,end,group)
        #res = self.cache.get(ckey,None)
        #if res is None:
        #    res = {}
        #    self.execute(sql)
        #    for i in self.client.fetchAll():
        #        arr = i.split('\t')
         #       key,val = ','.join(arr[1:]),int(arr[0])
          #      res[key] = val
           # self.cache[ckey] = res
        #keys,count = self.group_keys(**kargs),0
        #for i in keys:
        #    count += res.get(i,0)
        #return count


    def get_run_user_count(self, start, end, **kargs):
        ''' 
        get count of run user from hive table user_init_byday
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.or_sql('channel',[108,109,110,111])
        sub = self.merge_sql(sub,self.ds_sql_userset_init(start,end))
        group = self.group_userset_init(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(user))%s FROM userset_init WHERE %s %s"%(self.grp(group),sub,groupby)
        ckey = 'run_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val 
            self.cache[ckey] = res 
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_reserve_book_stat(self, start, end):
        '''
        reserve book to mysql
        '''
        #sql = "select * from reservebook where ds='%s'"%start.strftime('%Y-%m-%d')
        sql = "select ds,tds,count(distinct username) from reservebook where ds='%s' group by ds,tds" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_distinct_ip(self, start, end):
        '''
        get distinct ip 
        '''
        sql = "select distinct last_ip from userset_init_pool where ds='%s' and firstinittime>='%s' and firstinittime<'%s'" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_new_ips(self, start, end):
        '''
        get new ips
        '''
        thedaybeforeyest = start - datetime.timedelta(days=1)
        sql = "select a.last_ip from (select distinct last_ip from userset_init_pool where ds='%s' and firstinittime>='%s' and firstinittime<'%s') a left outer join (select ip from datamining.ip_province where ds='%s')b on a.last_ip = b.ip where b.ip is null" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),thedaybeforeyest.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_all_ips(self, start, end):
        '''
        get all ip
        '''
        thedaybeforeyest = start - datetime.timedelta(days=1)
        sql = "select distinct last_ip from userset_init_pool where ds='%s'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_userbandv6_stat(self, start, end):
        '''
        user band v6 to mysql
        '''
        sql = "select * from userband_v6 where ds='%s' and username !='' and username is not null" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_fujin_pvuv_stat(self, start, end):
        '''
        fujin pv uv stat to mysql
        '''
        sql = "select count(1),count(distinct username) from basic_service_v6 where key='FJ' and ds='%s'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_fujin_fee_stat(self, start, end):
        '''
        fujin fee stat to mysql
        '''
        sql = "select count(distinct a.username),sum(b.rechargingnum) from basic_service_v6 a join uc_down_recharge b on a.username=b.loginname where a.key='FJ' and a.ds='%s' and b.ds='%s' and a.username is not null and a.username != '' and a.username != 'unknown'" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_income_by_pk_stat(self, start, end):
        '''
        datamining.income by pk
        '''
        sql = """
        select pkname, count(distinct(concat(user_name,bid))) as pv,sum(realpay) as realpay, sum(b.recharge) as amount from 
        (select user_name,bid,fcid,
        (case when pk rlike 'WXC|WX' then '微信分享' 
        when pk rlike 'TR' then 'txt推荐'
        when (pk rlike '^[Tt][Cc]' or pk rlike '^2[Kk]' or pk rlike '^1[Kk]' or pk rlike '^136B' or pk rlike '^135B' ) then '分类' 
        when pk rlike '^SD' then '侧边栏' 
        when pk rlike '^[Qq][12]' then '书虫问答' 
        when pk='ps' or (pk='-' and fcid in (64,65,91,92,76,77,50,51,100,101,102)) then '弹窗' 
        when pk='-' and fcid in (200,201,202) then '用户升级赠送' 
        when pk='-' and fcid not in (64,65,91,92,76,77,50,51,100,101,102,200,201,202) and fcid >=2 then '内置' 
        when pk='-' and fcid in (-1,0,1) then '未知' 
        when pk rlike '[Gg][Uu]' then '猜你喜欢' 
        when pk rlike '^[Uu][Gg][Uu]' then '个人中心猜你喜欢' 
        when pk rlike 'FJ' then '附近的人'
        when pk rlike '^EM' then '事件营销榜单' 
        when pk rlike 'CTS' then '城市书房' 
        when pk rlike '^[Bb][Ee]' then '书架喇叭入口' 
        when pk rlike '8U1' or pk rlike '^7U' or pk rlike '4B26' then '包月' 
        when pk rlike '8B10' then 'cp特权引导页' 
        when pk rlike '^6B' then 'MORE图书列表' 
        when pk rlike '^5U' then '特权详情' 
        WHEN (pk ='5B4' or pk='4B4' or pk rlike '^5B10' or pk rlike '^4B10') THEN '书城首页' 
        when pk rlike '4U14' then '查看消息——广告' 
        when pk rlike '4B23' then '大家都在搜免费' 
        when pk rlike '4[Bb]13' then '活动问答' 
        when pk ='4B1' then '经典频道' 
        when (pk rlike '^2[Ss]' or pk rlike '^1[sS]') then '搜索' 
        when pk rlike '2Q2' then '进入翻牌页面' 
        when (pk rlike '^17[Bb]' or pk rlike '^12[Bb]' or pk rlike '18B1') then '简介页' 
        when (pk rlike '129[Bb]' or pk rlike '4B6' or pk rlike '^[pP][Rr]') then '专题' 
        when (pk rlike '128[Bb]' or pk rlike '^137B') then '排行' 
        when pk rlike '126[Bb]' then '标签' 
        when pk rlike '^11[Bb]' then '合作厂商' 
        when pk rlike '130B' then '大家都在看' 
        else '其他' end) pkname 
        from datamining.bookfirstpk 
        group by user_name,bid ,fcid,
        (case when pk rlike 'WXC|WX' then '微信分享' 
        when pk rlike 'TR' then 'txt推荐'
        when (pk rlike '^[Tt][Cc]' or pk rlike '^2[Kk]' or pk rlike '^1[Kk]' or pk rlike '^136B' or pk rlike '^135B' ) then '分类' 
        when pk rlike '^SD' then '侧边栏' 
        when pk rlike '^[Qq][12]' then '书虫问答' 
        when pk='ps' or (pk='-' and fcid in (64,65,91,92,76,77,50,51,100,101,102)) then '弹窗' 
        when pk='-' and fcid in (200,201,202) then '用户升级赠送' 
        when pk='-' and fcid not in (64,65,91,92,76,77,50,51,100,101,102,200,201,202) and fcid >=2 then '内置' 
        when pk='-' and fcid in (-1,0,1) then '未知' 
        when pk rlike '[Gg][Uu]' then '猜你喜欢'
        when pk rlike '^[Uu][Gg][Uu]' then '个人中心猜你喜欢'
        when pk rlike 'FJ' then '附近的人'
        when pk rlike '^EM' then '事件营销榜单' 
        when pk rlike 'CTS' then '城市书房' 
        when pk rlike '^[Bb][Ee]' then '书架喇叭入口' 
        when pk rlike '8U1' or pk rlike '^7U' or pk rlike '4B26' then '包月' 
        when pk rlike '8B10' then 'cp特权引导页' 
        when pk rlike '^6B' then 'MORE图书列表' 
        when pk rlike '^5U' then '特权详情' 
        WHEN (pk ='5B4' or pk='4B4' or pk rlike '^5B10' or pk rlike '^4B10') THEN '书城首页' 
        when pk rlike '4U14' then '查看消息——广告' 
        when pk rlike '4B23' then '大家都在搜免费' 
        when pk rlike '4[Bb]13' then '活动问答' 
        when pk ='4B1' then '经典频道' 
        when (pk rlike '^2[Ss]' or pk rlike '^1[sS]') then '搜索' 
        when pk rlike '2Q2' then '进入翻牌页面' 
        when (pk rlike '^17[Bb]' or pk rlike '^12[Bb]' or pk rlike '18B1') then '简介页' 
        when (pk rlike '129[Bb]' or pk rlike '4B6' or pk rlike '^[pP][Rr]') then '专题' 
        when (pk rlike '128[Bb]' or pk rlike '^137B') then '排行' 
        when pk rlike '126[Bb]' then '标签' 
        when pk rlike '^11[Bb]' then '合作厂商' 
        when pk rlike '130B' then '大家都在看' 
        else '其他' end))a 
        join (select loginname, bookid, sum(rechargingnum) as realpay, sum(rechargingnum)+sum(giftrechargingnum) recharge from uc_down_recharge where ds>='%s' and ds<'%s' group by loginname,bookid ) b on a.user_name = b.loginname and a.bid =b.bookid group by pkname
        """ % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_run_user_province_stat(self,start,end):
        '''
        run user province
        '''
        #sql = "SELECT regexp_extract(a.ireaderver,'(ireader_[0-9].[0-9])'),a.channel,b.province,COUNT(DISTINCT(a.user)) FROM userset_init a join datamining.ip_province b on a.ip=b.ip WHERE (a.channel like '108%%' OR a.channel like '109%%' OR a.channel like '110%%' OR a.channel like '111%%') AND a.ds>='%s' AND a.ds<'%s' and b.ds='%s' group by regexp_extract(a.ireaderver,'(ireader_[0-9].[0-9])'),a.channel,b.province" % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        sql = "SELECT regexp_extract(a.ireaderver,'(ireader_[0-9].[0-9])'),a.channel,b.province,COUNT(DISTINCT(a.user)) FROM userset_init a join datamining.ip_province b on a.ip=b.ip WHERE (a.channel like '108%%' OR a.channel like '109%%' OR a.channel like '110%%' OR a.channel like '111%%') AND a.ds>='%s' AND a.ds<'%s' and b.ds='%s' group by regexp_extract(a.ireaderver,'(ireader_[0-9].[0-9])'),a.channel,b.province" % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),'2014-03-20')
        self.execute(sql)
        return self.client.fetchAll()

    def get_visit_user_province_stat(self,start,end):
        '''
        visit user province
        '''
        #sql = "select regexp_extract(a.versionname,'(ireader_[0-9].[0-9])'),a.partnerid,b.province,COUNT(DISTINCT(a.username)) from basic_service_v6 a join datamining.ip_province b on a.ip=b.ip where (a.partnerid like '108%%' OR a.partnerid like '109%%' OR a.partnerid like '110%%' OR a.partnerid like '111%%') and a.ds>='%s' and a.ds<'%s' and b.ds='%s' and a.key in ('4B4','4B104','4B100','4B101','4B102','1K1','1P1','1U1','3U1','0R1','0R2','2S1','17B','128B','126B','129B') group by regexp_extract(a.versionname,'(ireader_[0-9].[0-9])'),a.partnerid,b.province"  % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        sql = "select regexp_extract(a.versionname,'(ireader_[0-9].[0-9])'),a.partnerid,b.province,COUNT(DISTINCT(a.username)) from basic_service_v6 a join datamining.ip_province b on a.ip=b.ip where (a.partnerid like '108%%' OR a.partnerid like '109%%' OR a.partnerid like '110%%' OR a.partnerid like '111%%') and a.ds>='%s' and a.ds<'%s' and b.ds='%s' and a.key in ('4B4','4B104','4B100','4B101','4B102','1K1','1P1','1U1','3U1','0R1','0R2','2S1','17B','128B','126B','129B') group by regexp_extract(a.versionname,'(ireader_[0-9].[0-9])'),a.partnerid,b.province"  % (start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),'2014-03-20')
        self.execute(sql)
        return self.client.fetchAll()

    def get_wechat_stat(self, start, end):
        '''
        wechat stat to mysql
        '''
        sql = "select bid,count(username),count(distinct username) from basic_down_v6 where ds='%s' and p_key = 'WX' group by bid" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_last_last_week_new_user_run_stat(self, start, end, yest):
        '''
        last_last_week_new_user_run
        '''
        sql = "SELECT count(DISTINCT user) FROM userset_init_pool WHERE (first_channel like '108%%' OR first_channel like '109%%' OR first_channel like '110%%' OR first_channel like '111%%') AND ds='%s' and firstinittime >='%s' and firstinittime <'%s'"%(yest.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_pvuv_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(1),count(distinct username) from basic_service_v6 where ds='%s' and params['ca'] ='Questions.Index'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_right_pvuv_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(1),count(distinct username) from bookworm where ds='%s' and status='0'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_wrong_pvuv_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(1),count(distinct username) from bookworm where ds='%s' and status='1'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_pay_pvuv_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(1),count(distinct username) from bookworm where ds='%s' and status='2'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_recharge_pvuv_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(1),count(distinct username) from bookworm where ds='%s' and status='3'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_amount_stat(self, start):
        '''
        get bookworm puuv stat
        '''
        sql = "select count(distinct a.username),sum(b.amount) from bookworm a join import_zy_recharging_order_seq b on a.username=b.usr and a.ds='%s' and b.ds='%s' and a.status='3'" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_pay_count_1(self, start):
        '''
        get bookworm pay count 1
        '''
        sql = "select count(1) from (select username,count(1)cnt from bookworm where ds='%s' and status=2 group by username having cnt=1)tmp" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_pay_count_2(self, start):
        '''
        get bookworm pay count 2
        '''
        sql = "select count(1) from (select username,count(1)cnt from bookworm where ds='%s' and status=2 group by username having cnt=2)tmp" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_bookworm_pay_count_3(self, start):
        '''
        get bookworm pay count 3
        '''
        sql = "select count(1) from (select username,count(1)cnt from bookworm where ds='%s' and status=2 group by username having cnt=3)tmp" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_stat(self, start):
        '''
        get recharge stat
        '''
        sql = "select b.channel,regexp_extract(b.ireaderver,'(ireader_[0-9].[0-9])'),sum(a.amount) from archive_v6_srv_recharge a join basic_recharge_v6 b on a.username=b.username where a.ds='%s' and b.ds='%s' and a.amount !=0 and a.status='1' group by b.channel,regexp_extract(b.ireaderver,'(ireader_[0-9].[0-9])')" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_detail(self, start):
        '''
        get recharge detail
        '''
        sql = "select count(distinct username),count(1),frombustype,channel,innerver,curbustype,recharging_type from recharge_v6_stat_username_join where (curbustype like 'alipay%%' or curbustype like 'bank%%' or curbustype like 'gameCard%%' or curbustype like 'mobileCard%%' or curbustype like 'sms%%' or curbustype like 'tenpay%%' or curbustype like 'tcl%%' or curbustype like 'huiben%%')  and frombustype in ('ad','book','month','order','sign','task','unknown','user') and ds='%s' group by frombustype,channel,innerver,curbustype,recharging_type" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_detail_order(self, start):
        '''
        get recharge detail order
        '''
        sql = "select count(distinct username),count(1),frombustype,channel,innerver,recharging_type from (select distinct orderid,username,amount,frombustype,channel,innerver,recharging_type from recharge_v6_stat_username_join where seq_type=1 and amount !=0 and frombustype in ('month','order','push','unknown','ad','book','sign','task','user') and ds='%s' )tmp group by frombustype,channel,innerver,recharging_type" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_user_run_innerver(self, start):
        sql = "select count(distinct user),channel,innerver from userset_init where ds='%s' and (channel like '108%%' OR channel like '109%%' OR channel like '110%%' OR channel like '111%%') group by channel,innerver" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_detail_finish(self, start):
        '''
        get recharge detail finish
        '''
        sql = "select count(distinct username),count(1),frombustype,channel,innerver,recharging_type,sum(amount),sum(gift_amount) from (select distinct orderid,username,amount,gift_amount,frombustype,channel,innerver,recharging_type from recharge_v6_stat_username_join where seq_type=3 and status=1 and amount !=0 and frombustype in ('month','order','push','unknown','ad','book','sign','task','user') and ds='%s')tmp group by frombustype,channel,innerver,recharging_type" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_amount_order(self, start):
        '''
        get recharge amount order
        '''
        sql = "select count(distinct username),count(1),frombustype,channel,innerver,amount,recharging_type from (select distinct orderid,username,amount,frombustype,channel,innerver,recharging_type from recharge_v6_stat_username_join where seq_type=1 and amount !=0 and frombustype in ('month','order','push','unknown','ad','book','sign','task','user') and ds='%s' )tmp group by frombustype,channel,innerver,amount,recharging_type" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_recharge_amount_finish(self, start):
        '''
        get recharge amount finish
        '''
        sql = "select count(distinct username),count(1),frombustype,channel,innerver,amount,recharging_type from (select distinct orderid,username,amount,gift_amount,frombustype,channel,innerver,recharging_type from recharge_v6_stat_username_join where seq_type=3 and status=1 and amount !=0 and frombustype in ('month','order','push','unknown','ad','book','sign','task','user') and ds='%s')tmp group by frombustype,channel,innerver,amount,recharging_type" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_pay_stat(self, start):
        '''
        get pay stat
        '''
        #sql = "select b.channel,regexp_extract(b.ireaderver,'(ireader_[0-9].[0-9])'),sum(a.rechargingnum),sum(a.giftrechargingnum) from uc_down_recharge a join userset_init b on a.loginname=b.username and a.ds='%s' and b.ds='%s' group by b.channel,regexp_extract(b.ireaderver,'(ireader_[0-9].[0-9])')" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        sql = "select b.channel,b.innerver,sum(a.rechargingnum),sum(a.giftrechargingnum) from uc_down_recharge a join userset_init b on a.loginname=b.username and a.ds='%s' and b.ds='%s' group by b.channel,b.innerver" % (start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_balance_stat(self, start):
        '''
        get balance stat
        '''
        sql = "select sum(recharge_balance),sum(gift_balance) from datamining.user_account where ds='%s'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_user_task_stat(self, start):
        '''
        get user task stat
        '''
        sql = "select ds,action_type,task_id,task_name,count(distinct user_name) from user_task where ds='%s' group by ds,action_type,task_id,task_name" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_all_user_task_stat(self, start):
        '''
        get all user task stat
        '''
        sql = "select time,user_name,action_type,task_id,task_name,ds,params from user_task where ds='%s'" % start.strftime('%Y-%m-%d')
        self.execute(sql)
        return self.client.fetchAll()

    def get_last_week_retention_stat(self, yest, llstart, llend, lstart, lend):
        '''
        last_week_retention_stat
        llstart = 2014-01-06
        llend = 2014-01-13
        lstart = 2014-01-13
        lend = 2014-01-20
        '''
        sql = """
        select count(1) from (SELECT DISTINCT user FROM userset_init_pool 
        WHERE (first_channel like '108%%' OR first_channel like '109%%' OR first_channel like '110%%' OR first_channel like '111%%') 
        AND ds='%s' and firstinittime >='%s' and firstinittime <'%s')a join 
        (select DISTINCT user from userset_init 
        where ds>='%s' and ds<'%s' and (channel like '108%%' OR channel like '109%%' OR channel like '110%%' OR channel like '111%%'))b 
        on a.user=b.user
        """%(yest.strftime('%Y-%m-%d'),llstart.strftime('%Y-%m-%d'),llend.strftime('%Y-%m-%d'),lstart.strftime('%Y-%m-%d'),lend.strftime('%Y-%m-%d'))
        self.execute(sql)
        return self.client.fetchAll()

    def get_all_one_week_fee(self, start, end):
        '''
        init arpu_one_week_fee one_week_fee
        '''
        #sql = "select sum(chargefee),0.4*sum(smsfee), to_date(firstinittime) from ana_tempdb.userset_init_pool_simp_username_att a join ana_tempdb.marpu_charge_by_uidds b on a.username=b.user_name where concat(substr(ds,0,4),'-',substr(ds,5,2),'-',substr(ds,7,2)) >= to_date(firstinittime) and concat(substr(ds,0,4),'-',substr(ds,5,2),'-',substr(ds,7,2)) < date_add(to_date(firstinittime),7) and firstinittime>='2013-01-01' group by to_date(firstinittime) "
        sql = "select sum(chargefee),0.4*sum(smsfee), to_date(firstvisittime) from ana_tempdb.userset_visit_pool_username_att a join ana_tempdb.marpu_charge_by_uidds b on a.username=b.user_name where concat(substr(ds,0,4),'-',substr(ds,5,2),'-',substr(ds,7,2)) >= to_date(firstvisittime) and concat(substr(ds,0,4),'-',substr(ds,5,2),'-',substr(ds,7,2)) < date_add(to_date(firstvisittime),7) and firstvisittime>='2013-01-01' group by to_date(firstvisittime)"
        self.execute(sql)
        return self.client.fetchAll()


    def get_all_one_week_new_user_visit(self, start, end):
        '''
        init arpu_one_week_fee new_user_visit
        '''
        #sql = "select count(distinct username ),to_date(firstinittime)ds from ana_tempdb.userset_init_pool_simp_username_att where firstinittime>='2013-01-01' group by to_date(firstinittime) order by ds asc"
        sql = "select count(distinct username ),to_date(firstvisittime)ds from ana_tempdb.userset_visit_pool_username_att where firstvisittime>='2013-01-01' group by to_date(firstvisittime) order by ds asc"
        self.execute(sql)
        return self.client.fetchAll()

    def get_arpu_one_day_new_user_run(self, start, end):
        '''
        get one day new user run 1.7.2
        '''
        last_week_start = end - datetime.timedelta(days=7)
        last_week_end = last_week_start + datetime.timedelta(days=1)
        _last_week_start = last_week_start.strftime('%Y-%m-%d')
        _last_week_end = last_week_end.strftime('%Y-%m-%d')
        yest = start.strftime('%Y-%m-%d')
        tody = end.strftime('%Y-%m-%d')
        
        sub = self.or_sql('first_channel',[108,109,110,111])
        sql = "SELECT COUNT(DISTINCT(user)) FROM userset_init_pool WHERE (%s) AND ds>='%s' AND ds<'%s' AND firstinittime regexp '^%s.*$' and regexp_extract(first_ireaderver,'(ireader_[0-9].[0-9].[0-9])')>='ireader_1.7.2'" % (sub, yest, tody, _last_week_start)
        self.execute(sql)
        return self.client.fetchAll()[0]

    def get_arpu_one_week_fee(self, start, end):
        '''
        get one week fee from uc_down_recharge and userset_init (user init)
        '''
        last_week_start = end - datetime.timedelta(days=7)
        last_week_end = last_week_start + datetime.timedelta(days=1)
        _last_week_start = last_week_start.strftime('%Y-%m-%d')
        _last_week_end = last_week_end.strftime('%Y-%m-%d')
        last_week_start_ = last_week_start.strftime('%Y%m%d')
        end_ = end.strftime('%Y%m%d')
        _end = end.strftime('%Y-%m-%d')
        sub = self.or_sql('first_channel',[108,109,110,111])
        yest = start.strftime('%Y-%m-%d')
        tody = end.strftime('%Y-%m-%d')
        sql = "SELECT round(SUM(a.rechargingnum),2)+0.4*(round(SUM(case a.prcode when '2' then cast(a.price as double) else 0.0 end),2)) FROM uc_down_recharge a join (SELECT DISTINCT(username)username FROM userset_init_pool WHERE (%s) AND ds>='%s' and not username in ('unknown','null','loading') AND ds<'%s' AND firstinittime regexp '^%s.*$' and regexp_extract(first_ireaderver,'(ireader_[0-9].[0-9].[0-9])')>='ireader_1.7.2' and first_ireaderver regexp '(ireader_[0-9].[0-9])')c on a.loginname=c.username WHERE a.ds>='%s' AND a.ds<'%s' " % (sub,yest,tody,_last_week_start,_last_week_start, _end)
        self.execute(sql)
        return self.client.fetchAll()[0]

    def get_arpu_30_days_fee(self, start, end):
        '''
        get 30 and 90 days fee from uc_down_recharge and userset_init_pool (user init)
        '''
        last_30_start = end - datetime.timedelta(days=30)
        last_30_end = last_30_start + datetime.timedelta(days=1)
        _last_30_start = last_30_start.strftime('%Y-%m-%d')
        _last_30_end = last_30_end.strftime('%Y-%m-%d')
        last_30_start_ = last_30_start.strftime('%Y%m%d')
        end_ = end.strftime('%Y%m%d')
        _end = end.strftime('%Y-%m-%d')
        sub = self.or_sql('first_channel',[108,109,110,111])
        yest = start.strftime('%Y-%m-%d')
        tody = end.strftime('%Y-%m-%d')
        sql = "SELECT round(SUM(a.rechargingnum),2)+0.4*(round(SUM(case a.prcode when '2' then cast(a.price as double) else 0.0 end),2)) FROM uc_down_recharge a join (SELECT DISTINCT(username)username FROM userset_init_pool WHERE (%s) AND ds>='%s' and not username in ('unknown','null','loading') AND ds<'%s' AND firstinittime regexp '^%s.*$' and regexp_extract(first_ireaderver,'(ireader_[0-9].[0-9].[0-9])')>='ireader_1.7.2' and first_ireaderver regexp '(ireader_[0-9].[0-9])')c on a.loginname=c.username WHERE a.ds>='%s' AND a.ds<'%s' " % (sub,yest,tody,_last_30_start,_last_30_start, _end)
        self.execute(sql)
        return self.client.fetchAll()[0]

    def get_arpu_90_days_fee(self, start, end):
        '''
        get 30 and 90 days fee from uc_down_recharge and userset_init_pool (user init)
        '''
        last_90_start = end - datetime.timedelta(days=90)
        last_90_end = last_90_start + datetime.timedelta(days=1)
        _last_90_start = last_90_start.strftime('%Y-%m-%d')
        _last_90_end = last_90_end.strftime('%Y-%m-%d')
        last_90_start_ = last_90_start.strftime('%Y%m%d')
        end_ = end.strftime('%Y%m%d')
        _end = end.strftime('%Y-%m-%d')
        sub = self.or_sql('first_channel',[108,109,110,111])
        yest = start.strftime('%Y-%m-%d')
        tody = end.strftime('%Y-%m-%d')
        sql = "SELECT round(SUM(a.rechargingnum),2)+0.4*(round(SUM(case a.prcode when '2' then cast(a.price as double) else 0.0 end),2)) FROM uc_down_recharge a join (SELECT DISTINCT(username)username FROM userset_init_pool WHERE (%s) AND ds>='%s' and not username in ('unknown','null','loading') AND ds<'%s' AND firstinittime regexp '^%s.*$' and regexp_extract(first_ireaderver,'(ireader_[0-9].[0-9].[0-9])')>='ireader_1.7.2' and first_ireaderver regexp '(ireader_[0-9].[0-9])')c on a.loginname=c.username WHERE a.ds>='%s' AND a.ds<'%s' " % (sub,yest,tody,_last_90_start,_last_90_start, _end)
        self.execute(sql)
        return self.client.fetchAll()[0]

    def get_arpu_one_week_new_user_visit(self, start, end):
        '''
        get one week fee from 
        '''
        last_week_start = end - datetime.timedelta(days=7)
        last_week_end = last_week_start + datetime.timedelta(days=1)
        _last_week_start = last_week_start.strftime('%Y-%m-%d')
        _last_week_end = last_week_end.strftime('%Y-%m-%d')
        yest = start.strftime('%Y-%m-%d')
        tody = end.strftime('%Y-%m-%d')
        sub = self.or_sql('b.first_channel',[108,109,110,111])
        sql = "SELECT COUNT(distinct(concat(a.user,a.username,a.regtype))) FROM userset_visit_pool a join userset_init_pool b on a.username=b.username WHERE (%s) AND (b.ds>='%s' AND b.ds<'%s') AND (a.ds>='%s' AND a.ds<'%s') AND not a.username in ('unknown','null','loading') AND not b.username in('unknown','null','') AND a.firstvisittime regexp '^%s.*$' and regexp_extract(last_ireaderver,'(ireader_[0-9].[0-9].[0-9])')>='ireader_1.7.2' " % (sub,yest,tody,yest,tody,_last_week_start)
        self.execute(sql)
        return self.client.fetchAll()[0]

    def get_new_runuser_count(self, start, end, **kargs):
        '''
        get count of running-ireader user from hive table userset_init_pool
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        version_name = kargs.get('version_name',None)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        if version_name in ['','ireader_1.6','ireader_1.7','ireader_1.8','ireader_2.3','ireader_2.4','ireader_2.6','ireader_2.7','ireader_3.0','ireader_3.1','ireader_3.2']:
            #sub = self.or_sql('first_channel',[108,109,110,111])
            sub = ''
            #sub = self.merge_sql(sub,self.ds_sql_userset_init_pool(start,end))
            sub = self.merge_sql(sub,self.ds_sql_userset_init_pool(start,end))
            #sub = self.merge_sql(sub,self.regexp_firstinittime(start))
            sub = self.merge_sql(sub,self.regexp_fitime(start))
            #group = self.group_userset_init_pool(**kargs)
            group = self.group_userfirstinit(**kargs)
            groupby = ('GROUP BY %s' % group) if group else ''
            sql = "SELECT COUNT(DISTINCT(user))%s FROM userfirstinit WHERE %s %s"%(self.grp(group),sub,groupby)
            ckey = 'new_run_ucnt_%s_%s_%s'%(start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                self.execute(sql)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,val = ','.join(arr[1:]),int(arr[0])
                    res[key] = val
                self.cache[ckey] = res
            keys,count = self.group_keys(**kargs),0
            for i in keys:
                count += res.get(i,0)
            return count
        else:
            #for version_name above android 2.0 
            group2 = self.group_basic_service_v6(**kargs)
            groupby2 = ('GROUP BY %s' % group2) if group2 else ''
            s,e = start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')
            sub2 = "act='autoRegister' AND uri RLIKE 'api.php' and ds>='%s' and ds<'%s'"%(s,e) 
            sql2 = "SELECT COUNT(distinct imei)%s FROM basic_service_v6 WHERE %s %s"%(self.grp(group2),sub2,groupby2)
            ckey = 'new_run_ucnt_%s_%s_%s'%(s,e,group2)
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                self.execute(sql2)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,val = ','.join(arr[1:]),int(arr[0])
                    res[key] = val
                self.cache[ckey] = res
            keys,count = self.group_keys(**kargs),0
            for i in keys:
                count += res.get(i,0)
            return count

    def get_new_visuser_count_old(self, start, end, **kargs):
        '''
        get count of visiting-ireader user from hive table userset_visit_pool and userset_init_pool
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = "first_vis_time>='%s' AND first_vis_time<'%s'"%(start,end)
        sub = self.merge_sql(sub,self.or_sql('partner_id',[108,109,110,111]))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(uid))%s FROM userbasic_v6_new WHERE %s %s" % (self.grp(group),sub,groupby)
        ckey = 'new_vis_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_new_visuser_count(self, start, end, **kargs):
        '''
        get count of visiting-ireader user from hive table userbasic_v6_new
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.or_sql('b.first_channel',[108,109,110,111])
        sub += " AND (b.ds>='%s' AND b.ds<'%s') AND (a.ds>='%s' AND a.ds<'%s') AND not a.username in ('unknown','null','') AND not b.username in('unknown','null','')" % (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d') ,start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        sub = self.merge_sql(sub,self.regexp_afirstvisittime(start))
        group = self.group_userset_init_pool(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(distinct(concat(a.user,a.username,a.regtype)))%s FROM userset_visit_pool a join userset_init_pool b on a.username=b.username WHERE %s %s" % (self.grp(group),sub,groupby)
        ckey = 'new_vis_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_active_user_count(self, start, end, **kargs):
        '''
        get count of active user
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = "key='17B'"
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(%s))%s FROM service_v6 WHERE %s %s"%(self.uid(),self.grp(group),sub,groupby)
        ckey = 'active_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_pv(self, start, end, **kargs):
        '''
        get PV
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        keylist = ['1R1','1P1','1W1','1S1', '1K','1U','2U','3U','5U','6U','8U','9U','10U','11U',
                   '17B','128B','126B','127B','4B','129B']
        sub = self.or_sql('key',keylist)
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(1)%s FROM service_v6 WHERE %s %s"%(self.grp(group),sub,groupby)
        ckey = 'pv_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count
    
    def get_imei_count(self, start, end, **kargs):
        '''
        get count of imei
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(imei))%s FROM service_v6 WHERE %s %s" % (self.grp(group),sub,groupby)
        ckey = 'imei_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def sql_for_pagetype(self, pt):
        '''
        pt: page type, which is in PAGE_TYPE
        '''
        sub = ''
        if pt in PAGE_TYPE:
            keys = ','.join(["'%s'"%i for i in PAGE_TYPE[pt][0]])
            if pt == 'recharge':
                sub = "key regexp '^0R(\\\d)$'" #recharge 0R1 0R2
            else:
                sub = "key in (%s)"%(keys,)
            if pt == 'search':
                sub = "(%s AND search='')"%sub
        return sub

    def get_pagetype_count(self, start, end, **kargs):
        '''
        stat for some kinds of page types
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        for t in PAGE_TYPE:
            where = self.merge_sql(sub,self.sql_for_pagetype(t))
            sql = "SELECT COUNT(1),COUNT(DISTINCT(%s))%s FROM service_v6 WHERE %s %s"%(self.uid(),self.grp(group),where,groupby)
            ckey = 'pagetype_%s_%s_%s_%s'%(t,start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                self.execute(sql)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,pv,uv = ','.join(arr[2:]),int(arr[0]),int(arr[1])
                    res[key] = (pv,uv)
                self.cache[ckey] = res
            keys,pv,uv= self.group_keys(**kargs),0,0
            for i in keys:
                _pv,_uv = res.get(i,(0,0))
                pv += _pv
                uv += _uv
            yield {'type':t,'pv':pv,'uv':uv}

    def get_topN(self, start, end, num=100, **kargs):
        '''
        top N for some kinds of page types
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        for t in TOP_TYPE:
            where = self.merge_sql(sub,"key='%s'"%TOP_TYPE[t][0])
            qtype = "SELECT %s,COUNT(1) AS cnt,COUNT(DISTINCT(%s)) %s"%(TOP_TYPE[t][1],self.uid(),self.grp(group))
            sql = "%s FROM service_v6 WHERE %s GROUP BY %s%s"%(qtype,where,TOP_TYPE[t][1],self.grp(group))
            ckey = 'topn_eachpt_%s_%s_%s_%s'%(t,start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                self.execute(sql)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,val,pv,uv = ','.join(arr[3:]),arr[0].strip(),int(arr[1]),int(arr[2])
                    res.setdefault(key,[]).append((val,pv,uv))
                self.cache[ckey] = res
            keys,rdict = self.group_keys(**kargs),{}
            for i in keys:
                for r in res.get(i,[]):
                    val,pv,uv = r[0].strip(),r[1],r[2]
                    if t in ('board','subject') and not val.isdigit():
                        continue
                    if val in rdict:
                        rdict[val]['pv'] += pv
                        rdict[val]['uv'] += uv
                    else:
                        rdict[val] = {'val':val,'pv':pv,'uv':uv}
            rlist = rdict.values()
            rlist.sort(cmp=lambda x,y:cmp(x['pv'],y['pv']),reverse=True)
            yield {'type':t,'list':rlist[:num] if num else rlist}

    def get_topN_book(self, start, end, pkeys=[], **kargs):
        '''
        top N for books according to pv
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        if pkeys:
            pkeys = ','.join(["'%s'"%i for i in pkeys])
            sub = self.merge_sql(sub,"p_key in (%s)"%pkeys)
        keys = ','.join(["'%s'"%i for i in PAGE_TYPE['bkintro'][0]])
        where = self.merge_sql(sub,"key in (%s)"%keys)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        qtype = "SELECT feature_id,COUNT(1),COUNT(DISTINCT(%s))%s"%(self.uid(),self.grp(group))
        sql = "%s FROM service_v6 WHERE %s GROUP BY feature_id %s"%(qtype,where,self.grp(group))
        ckey = 'topn_book_%s_%s_%s_%s'%(start,end,pkeys,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                try:
                    key,bid,pv,uv = ','.join(arr[3:]),arr[0].strip(),int(arr[1]),int(arr[2])
                except Exception,e:
                    logging.error('%s,%s\n',i,str(e),exc_info=True)
                    continue
                res.setdefault(key,[]).append((bid,pv,uv))
            self.cache[ckey] = res
        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,pv,uv = r[0],r[1],r[2]
                if bid in rdict:
                    rdict[bid]['pv'] += pv
                    rdict[bid]['uv'] += uv
                else:
                    rdict[bid] = {'bid':bid,'pv':pv,'uv':uv}
        return rdict
     
    def get_topN_product(self, start, end, num=9, **kargs):
        '''
        top N machines by user id
        start: start time
        end: end time
        num: number of product name
        kargs: run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,"product_name<>''")
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        qtype = "SELECT COUNT(1),COUNT(DISTINCT(%s)),product_name%s"%(self.uid(),self.grp(group))
        sql = "%s FROM service_v6 WHERE %s GROUP BY product_name%s " % (qtype,sub,self.grp(group))
        ckey = 'topu_product_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,pv,uv,name = ','.join(arr[3:]),int(arr[0]),int(arr[1]),arr[2].strip()
                res.setdefault(key,[]).append((name,pv,uv))
            self.cache[ckey] = res
        keys,rdict,pv_total,uv_total = self.group_keys(**kargs),{},0,0
        for i in keys:
            for r in res.get(i,[]):
                name,pv,uv = r[0],r[1],r[2]
                pv_total += pv
                uv_total += uv
                if name in rdict:
                    rdict[name]['pv'] += pv
                    rdict[name]['uv'] += uv
                else:
                    rdict[name] = {'name':name,'type':'user','pv':pv,'uv':uv}
        rlist = rdict.values()
        rlist.sort(cmp=lambda x,y:cmp(x['uv'],y['uv']),reverse=True)
        if num:
            rlist = rlist[:num] 
        other = 0
        for i in rlist:
            i['ratio'] = i['uv']/uv_total if uv_total else 0
            other += i['uv']
        rlist.append({'name':'other','type':'user','uv':other,'ratio':other/uv_total if uv_total else 0})
        return uv_total,rlist

if __name__ == '__main__':
    s = ServiceQuery('192.168.0.144','10000')
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
    #now = now - datetime.timedelta(days=3)
    end = now 
    start = now - datetime.timedelta(days=1)
    #print now
    version_name = "ireader_2.4" 
    #partner_id = 108716
    #product_name = 'GT-I9300'  
    #run_id = 501607
    #plan_id = 34 
    #kargs = {'version_name':version_name,'run_id':run_id,'product_name':product_name,'plan_id':plan_id}
    kargs = {'version_name':version_name,'partner_id':partner_id}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
#    kargs = {'scope_id':82694,'mode':'week'}
#    print 'nu7arpu',s.get_arpu_7days_stat(start,end)
#    print 'retention户数\t',s.get_retention_user_count(start,end,**kargs)
    print '启动用户数\t',s.get_run_user_count(start,end,**kargs)
#    print '新增启动用户数\t',s.get_new_runuser_count(start,end,**kargs)
#    print '访问用户数\t',s.get_vis_user_count(start,end,**kargs)
#    print '新增访问用户数\t',s.get_new_visuser_count(start,end,**kargs)
#    print '活跃用户数\t',s.get_active_user_count(start,end,**kargs)
#    print '访问PV\t',s.get_pv(start,end,**kargs)
#    print 'imei数\t',s.get_imei_count(start,end,**kargs)
#    print '各类型页面统计1\t',[i for i in s.get_pagetype_count(start,end,**kargs)]
#    print '各类型页面统计2\t',[i for i in s.get_pagetype_count(start,end,**kargs)]
#    print '各类型页面统计3\t',[i for i in s.get_pagetype_count(start,end,version_name='ireader_1.7')]
#    print '各类型页面统计4\t',[i for i in s.get_pagetype_count(start,end,version_name='ireader_1.8')]
#    print '各类型TopN\t',[i for i in s.get_topN(start,end,num=10,**kargs)]
#    print '各类型TopN 2\t',[i for i in s.get_topN(start,end,num=10,version_name='ireader_1.8')]
#    print 'topN books\t',s.get_topN_book(start,end,100,**kargs)
#   print 'topN books 2\t',s.get_topN_book(start,end,**kargs)
#    print 'topN product\t',s.get_topN_product(start,end,num=9,**kargs)
#    print 'topN product 2\t',s.get_topN_product(start,end,num=9,version_name='ireader_1.8')
    print datetime.datetime.now() - now

