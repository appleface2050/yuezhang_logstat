#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from lib.utils import strptime,time_next
from model.factory import Proportion,Partner
class Scope(Model):
    '''
    scope model: dimension
    '''
    _db = 'logstatV2'
    _table = 'scope'
    _pk = 'id'
    _fields = set(['id','platform_id','run_id','plan_id','partner_id','version_name','product_name',
                   'province','status','mask','modes','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`platform_id` int NOT NULL DEFAULT '0'", # 4, 5, 6
               "`run_id` int NOT NULL DEFAULT '0'",
               "`plan_id` int NOT NULL DEFAULT '0'",
               "`partner_id` int NOT NULL DEFAULT '0'",
               "`version_name` varchar(32) NOT NULL DEFAULT ''",
               "`product_name` varchar(64) NOT NULL DEFAULT ''",
               "`province` varchar(64) NOT NULL DEFAULT ''",
               "`status` enum('hide','pas','nice') NOT NULL DEFAULT 'pas'",
               "`mask` set('basic','visit','topn','book','product') NOT NULL DEFAULT 'basic'",
               "`modes` set('hour','day','week','month','year') NOT NULL DEFAULT 'day'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `platfrom_run_part_product_province` (`platform_id`,`run_id`,`plan_id`,`partner_id`,`version_name`,`product_name`,`province`)")
    
    def get_scope_ids_by_partner_ids_and_version_name(self,partner_id_list,version_name,partner_id=None):
        if partner_id_list:
            partner_id_list = [str(i) for i in partner_id_list]
        str_partner_id_list = ','.join(partner_id_list)
        res = {}
        qtype = "SELECT id AS scope_id,partner_id"
        ext = "partner_id IN (%s) AND version_name = '%s' and product_name = ''" % (str_partner_id_list,version_name)
        q = self.Q(qtype=qtype).extra(ext).groupby("partner_id")
        if partner_id:
            q = q.filter(partner_id=partner_id)
        return q 

    def get_scopes_by_parnter_id_for_update_new_user_run(self, parnter_id):                                                                                             
        if parnter_id:
            res = {}
            qtype = "SELECT id"
            ext = "partner_id =%s AND product_name = '' AND version_name <> '' AND run_id = 0 AND plan_id = 0" % parnter_id
            q = self.Q(qtype=qtype).extra(ext)
            res = q 
            return res 

    def get_ios_scope_id_list(self):
        qtype = "SELECT id"
        ext = "partner_id >=111000 AND partner_id LIKE '111%' AND version_name = '' AND product_name = '' AND STATUS ='pas' AND run_id = 0"
        q = self.Q(qtype=qtype).extra(ext)
        res = q
        return res

class Partner(Model):
    '''
    logstatV2.partner model
    '''
    _db = 'logstatV2'
    _table = 'partner'
    _pk = 'id'
    _fields = set(['id','partner_id','factory_id','uptime'])

    def get_all_partner(self):
        res = {}
        qtype = "SELECT partner_id"
        ext = "partner_id LIKE '108%' OR partner_id LIKE '109%' OR partner_id LIKE '110%' OR partner_id LIKE '111%'"
        q = self.Q(qtype=qtype).extra(ext)
        res = q
        return res

class BasicStat(Model):
    '''
    basic stat model
    '''
    _db = 'logstatV2'
    _table = 'basic_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','visits','imei','user_run','new_user_run','user_visit',
                   'new_user_visit','active_user_visit','user_retention','pay_user','cpay_down','cfree_down','bpay_down',
                   'bfree_down','cpay_user','cfree_user','bpay_user','bfree_user','cfee','bfee','recharge','batch_pv','batch_uv','batch_fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month','year') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`visits` int NOT NULL DEFAULT '0'",
               "`imei` int NOT NULL DEFAULT '0'",
               "`user_run` int NOT NULL DEFAULT '0'",
               "`new_user_run` int NOT NULL DEFAULT '0'",
               "`user_visit` int NOT NULL DEFAULT '0'",
               "`new_user_visit` int NOT NULL DEFAULT '0'",
               "`active_user_visit` int NOT NULL DEFAULT '0'",
               "`user_retention` int NOT NULL DEFAULT '0'",
               "`pay_user` int NOT NULL DEFAULT '0'",
               "`cpay_down` int NOT NULL DEFAULT '0'",
               "`cfree_down` int NOT NULL DEFAULT '0'",
               "`bpay_down` int NOT NULL DEFAULT '0'",
               "`bfree_down` int NOT NULL DEFAULT '0'",
               "`cpay_user` int NOT NULL DEFAULT '0'",
               "`cfree_user` int NOT NULL DEFAULT '0'",
               "`bpay_user` int NOT NULL DEFAULT '0'",
               "`bfree_user` int NOT NULL DEFAULT '0'",
               "`cfee` float NOT NULL DEFAULT '0'",
               "`bfee` float NOT NULL DEFAULT '0'",
               "`recharge` varchar(255) NOT NULL DEFAULT ''",
               "`batch_pv` int NOT NULL DEFAULT '0'",
               "`batch_uv` int NOT NULL DEFAULT '0'",
               "`batch_fee` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_time` (`scope_id`,`mode`,`time`)")
    _fieldesc= {'id':'ID','scope_id':'维度ID','mode':'统计模式','time':'时间','visits':'PV',
                'imei':'IMEI数','user_run':'启动用户','new_user_run':'新增启动用户','user_visit':'访问用户',
                'new_user_visit':'新增访问用户','active_user_visit':'活跃用户','user_retention':'留存用户',
                'pay_user':'付费用户','cpay_down':'按章付费下载','cfree_down':'按章免费下载',
                'bpay_down':'按本付费下载','bfree_down':'按本免费下载','cpay_user':'按章付费用户',
                'cfree_user':'按章免费用户','bpay_user':'按本付费用户','bfree_user':'按本免费用户',
                'cfee':'按章月饼消费','bfee':'按本月饼消费','recharge':'充值信息','batch_pv':'批量下载PV','batch_uv':'批量下载UV','batch_fee':'批量下载消费','uptime':'更新时间'}
    
    def get_table(self, **kargs):
        dtime = kargs["time"]
        if isinstance(dtime,str):
            dtime = strptime(dtime, "%Y-%m-%d %H:%M:%S")
        return "%s_%d_%d" % (self._table,dtime.year,dtime.month)

    def init_table(self, year):
        for month in range(1,13):
            super(BasicStat,self).init_table(time='%d-%d-01 00:00:00'%(year,month))

    def get_one_day_partner_stat(self, mode, start):
        excludes = ('id','uptime') 
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        qtype = "SELECT %s" % (sub)
        ext = "time='%s' and mode = '%s' AND scope_id IN (SELECT id FROM scope WHERE partner_id !=0)" % (start.strftime("%Y-%m-%d"),mode)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

    def get_recharge_by_multi_scope(self, scopeid_list, mode, start, end, ismean=False):
        sub1 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',1) as decimal)) consumefee"
        sub2 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',-2)as decimal)) msgfee"
        sum,count = {'consumefee':0,'msgfee':0},0
        scopeid_list = ','.join([str(i) for i in scopeid_list])
        if not scopeid_list:
            return sum
        while start < end:
            _end = min(time_next(start,'month'),end)
            qtype = 'SELECT count(distinct(time)) cnt,%s,%s' % (sub1,sub2)
            ext = "time>='%s' AND time<'%s' AND scope_id in (%s)"%(start,_end,scopeid_list)
            s = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)[0]
            count += s['cnt']
            for k in s:
                if k != 'cnt':
                    sum[k] += s[k] if s[k] else 0
            start = _end
        if ismean:
            for k in sum:
                sum[k] = int(float(sum[k])/count) if count else 0
        sum['msgfee'] = float(sum['msgfee'])*0.4
        return sum

    def get_recharge_by_multi_scope_proportion(self, scope_proportion_list, mode, start, end, ismean=False):
        sum,count = {'consumefee':0,'msgfee':0},0
        if not scope_proportion_list:
            return sum
        for i in scope_proportion_list:
            sub1 = "%s*(cast(SUBSTRING_INDEX(recharge,'/',1) as decimal)) consumefee"%i['proportion']
            sub2 = "%s*(cast(SUBSTRING_INDEX(recharge,'/',-2)as decimal)) msgfee"%i['proportion']
            qtype = 'SELECT %s,%s' % (sub1,sub2)
            ext = "time>='%s' AND time<'%s' AND scope_id=%s"%(start,end,i['scope_id'])
            s = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)[0]
            if s:
                for k in s:
                    sum[k] += s[k] if s[k] else 0
        #print sum
        sum['msgfee'] = float(sum['msgfee'])*0.4
        return sum

    def get_recharge_by_multi_scope_one_day(self, scopeid_list, time, mode='day', ismean=False):
        sub1 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',1) as decimal)) consumefee"
        sub2 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',-2)as decimal)) msgfee"
        sum,count = {'consumefee':0,'msgfee':0},0
        scopeid_list = ','.join([str(i) for i in scopeid_list])
        if not scopeid_list:
            return 0
        qtype = 'SELECT count(distinct(time)) cnt,%s,%s' % (sub1,sub2)
        ext = "scope_id in (%s)"%(scopeid_list)
        s = self.Q(qtype=qtype,time=time).filter(mode=mode,time=time).extra(ext)[0]
        count += s['cnt']
        for k in s:
            if k != 'cnt':
                sum[k] += s[k] if s[k] else 0
        if ismean:
            for k in sum:
                sum[k] = int(float(sum[k])/count) if count else 0
        sum['msgfee'] = float(sum['msgfee'])*0.4
        return sum['msgfee'] + float(sum['consumefee'])

    def get_data_by_multi_scope(self, scopeid_list, mode, start, end, ismean=False):
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        sub = ','.join(['SUM(%s)%s'%(i,i) for i in self._fields if i not in excludes])
        sum,count = dict([(i,0) for i in self._fields if i not in excludes]),0
        scopeids= ','.join([str(i) for i in scopeid_list])
        sum['consumefee'],sum['msgfee'] = 0,0
        if not scopeids:
            return sum
        _start = start
        while _start < end:
            _end = min(time_next(_start,'month'),end)
            qtype = 'SELECT count(distinct(time)) cnt, %s'%sub
            ext = "time>='%s' AND time<'%s' AND scope_id in (%s)"%(_start,_end,scopeids)
            s = self.Q(qtype=qtype,time=_start).filter(mode=mode).extra(ext)[0]
            count += s['cnt']
            for k in s:
                if k != 'cnt':
                    sum[k] += s[k] if s[k] else 0
            _start = _end
        if ismean:
            for k in sum:
                sum[k] = int(float(sum[k])/count) if count else 0
        recharge = self.get_recharge_by_multi_scope(scopeid_list,mode,start,end,ismean)
        sum['consumefee'],sum['msgfee'] = recharge['consumefee'],recharge['msgfee']
        sum['batch_fee'] = int(sum['batch_fee'])
        return sum

    def get_data_by_multi_scope_proportion(self, scope_proportion_list, mode, start, end, ismean=False):
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        sub = ','.join([str(i) for i in self._fields if i not in excludes])
        sum,count = dict([(i,0) for i in self._fields if i not in excludes]),0
        sum['consumefee'],sum['msgfee'] = 0,0
        if not scope_proportion_list:
            return sum
        for i in scope_proportion_list:
            temp = []
            for j in self._fields:
                if j not in excludes:
                    temp.append('('+str(i['proportion'])+'*'+str(j)+')'+str(j))
            sub = ','.join(temp)
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' AND scope_id=%s"%(start,end,i['scope_id'])
            s = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)[0]
            if s:
                for k in s:
                    sum[k] += s[k] if s[k] else 0
        #print sum
        recharge = self.get_recharge_by_multi_scope_proportion(scope_proportion_list,mode,start,end,ismean)
        sum['consumefee'],sum['msgfee'] = recharge['consumefee'],recharge['msgfee']
        sum['batch_fee'] = int(sum['batch_fee'])
        return sum

    def get_all_data_by_multi_scope(self, scope_id_list, start, end):
        if scope_id_list:
            scope_id_list = [str(i) for i in scope_id_list]
        str_scope_id_list = ','.join(scope_id_list)
        res = {}
        qtype = "SELECT scope_id,user_run,new_user_run,user_visit,new_user_visit,pay_user,active_user_visit,visits,cpay_down,bpay_down,cpay_user,bpay_user,cfree_down,bfree_down,cfree_user,bfree_user,cfee,bfee,batch_fee,batch_pv,batch_uv"
        ext = "scope_id IN (%s)" % str_scope_id_list
        q = self.Q(qtype=qtype,time=end).filter(mode='day').filter(time=end).extra(ext)
        return q

    def get_sum_data_by_multi_scope(self, scope_id_list, time):
        qtype = """
            SELECT ifnull(sum(user_run),0)user_run,ifnull(sum(new_user_run),0)new_user_run,ifnull(sum(user_visit),0)user_visit,
            ifnull(sum(new_user_visit),0)new_user_visit,ifnull(sum(pay_user),0)pay_user,ifnull(sum(active_user_visit),0)active_user_visit,
            ifnull(sum(visits),0)visits,ifnull(sum(cpay_down),0)cpay_down,ifnull(sum(bpay_down),0)bpay_down,
            ifnull(sum(cpay_user),0)cpay_user,ifnull(sum(bpay_user),0)bpay_user,ifnull(sum(cfree_down),0)cfree_down,ifnull(sum(bfree_down),0)bfree_down,
            ifnull(sum(cfree_user),0)cfree_user,
            ifnull(sum(bfree_user),0)bfree_user,ifnull(sum(cfee),0)cfee,ifnull(sum(bfee),0)bfee,ifnull(sum(batch_fee),0)batch_fee,ifnull(sum(batch_pv),0)batch_pv,
            ifnull(sum(batch_uv),0)batch_uv
            """
        ext = "scope_id IN (%s) and mode='day' and time='%s'" % (scope_id_list,time.strftime('%Y-%m-%d'))
        q = self.Q(qtype=qtype,time=time).extra(ext)
        return q

    def get_peak_recharge_by_multi_scope(self, scopeid_list, mode, start, end, ismax=True):
        t,func = 'MAX' if ismax else 'MIN',max if ismax else min
        sub1 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',1) as decimal)) consumefee"
        sub2 = "SUM(cast(SUBSTRING_INDEX(recharge,'/',-1)as decimal)) msgfee"
        peak,count = {'consumefee':0,'msgfee':0},0
        scopeids= ','.join([str(i) for i in scopeid_list])
        if not scopeids:
            return peak 
        while start < end:
            _end = min(time_next(start,'month'),end)
            qtype = 'SELECT %s,%s'%(sub1,sub2)
            ext = "time>='%s' AND time<'%s' AND scope_id in (%s)"%(start,_end,scopeids)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext).groupby('time')
            for p in q:
                for k in p:
                    peak[k] = int(func(peak[k],p[k])) if p[k] else 0
            start = _end
        peak['msgfee'] = float(peak['msgfee'])*0.4
        return peak

    def get_peak_by_multi_scope(self, scopeid_list, mode, start, end, ismax=True):
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        t,func = 'MAX' if ismax else 'MIN',max if ismax else min
        sub = ','.join(['SUM(%s)%s'%(i,i) for i in self._fields if i not in excludes])
        peak = dict([(i,0) for i in self._fields if i not in excludes])
        scopeids= ','.join([str(i) for i in scopeid_list])
        if not scopeids:
            return peak 
        _start = start
        while _start < end:
            _end = min(time_next(_start,'month'),end)
            qtype,ext = 'SELECT %s'%sub,"time>='%s' AND time<'%s' AND scope_id in (%s)"%(_start,_end,scopeids)
            q = self.Q(qtype=qtype,time=_start).filter(mode=mode).extra(ext).groupby('time')
            for p in q:
                for k in p:
                    peak[k] = int(func(peak[k],p[k])) if p[k] else 0
            _start = _end
        recharge = self.get_peak_recharge_by_multi_scope(scopeid_list,mode,start,end,ismax)
        peak['consumefee'],peak['msgfee'] = recharge['consumefee'],recharge['msgfee']
        return peak

    def get_data(self, scope_id, mode, start, end, ismean=False):
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        sub = ','.join(['SUM(%s)%s'%(i,i) for i in self._fields if i not in excludes])
        sum,count = dict([(i,0) for i in self._fields if i not in excludes]),0
        while start < end:
            _end = min(time_next(start,'month'),end)
            qtype,ext = 'SELECT %s'%sub,"time>='%s' AND time<'%s'"%(start,_end)
            q = self.Q(qtype=qtype,time=start).filter(scope_id=scope_id,mode=mode).extra(ext)
            count += q.count()
            s = q[0]
            for k in s:
                sum[k] += s[k] if s[k] else 0
            start = _end
        if ismean:
            for k in sum:
                sum[k] = int(float(sum[k])/count) if count else 0
        return sum

    def get_peak(self, scope_id, mode, start, end, ismax=True):
        excludes = ('id','scope_id','mode','time','recharge','uptime')
        t,func = 'MAX' if ismax else 'MIN',max if ismax else min
        sub = ','.join(['%s(%s)%s'%(t,i,i) for i in self._fields if i not in excludes])
        peak = dict([(i,0) for i in self._fields if i not in excludes])
        while start < end:
            _end = min(time_next(start,'month'),end)
            qtype,ext = 'SELECT %s'%sub,"time>='%s' AND time<'%s'" % (start,_end)
            p = self.Q(qtype=qtype,time=start).filter(scope_id=scope_id,mode=mode).extra(ext)[0]
            for k in p:
                peak[k] = int(func(peak[k],p[k])) if p[k] else 0
            start = _end
        return peak

    def get_model_data(self, scopeid_list, mode, start, end, p=1, psize=20, order_field='user_visit'):
        excludes = ('id','mode','time','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' and scope_id in (%s)" % (start,end,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)
            count = q.count()
            dlist = q.orderby(order_field,'DESC')[(p-1)*psize:p*psize]
        return {'count':count,'list':dlist}

    def get_all_model_data(self, scopeid_list, mode, start, end):
        excludes = ('id','mode','time','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' and scope_id in (%s)" % (start,end,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)
            count = q.count()
            dlist = q
        return {'count':count,'list':dlist}

    def get_one_day_model_data(self, scopeid_list, mode, start):
        excludes = ('id','mode','time','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time='%s' and scope_id in (%s)" % (start,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)
            count = q.count()
            dlist = q
        return {'count':count,'list':dlist}

    def get_one_day_model_data_proportion(self, proportion, scopeid_list, mode, start):
        excludes = ('scope_id','id','mode','time','recharge','uptime')
        sub = ','.join(['CAST(%s*%s AS SIGNED)%s'%(proportion,i,i) for i in self._fields if i not in excludes])
        sub = 'scope_id,' + sub
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time='%s' and scope_id in (%s)" % (start,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)
            count = q.count()
            dlist = q
        return dlist

    def get_one_day_multy_scope_stat(self, scopeid_list, mode, time):
        if scope_id_list:
            scope_id_list = [str(i) for i in scope_id_list]
        str_scope_id_list = ','.join(scope_id_list)
        res = {}
        qtype = "SELECT scope_id,user_run,new_user_run,user_visit,new_user_visit,pay_user,active_user_visit,visits,cpay_down,bpay_down,cpay_user,bpay_user,cfree_down,bfree_down,cfree_user,bfree_user,cfee,bfee,batch_fee,batch_pv,batch_uv"
        ext = "scope_id IN (%s)" % str_scope_id_list
        q = self.Q(qtype=qtype,time=end).filter(mode='day').filter(time=end).extra(ext)
        return q

    def get_multi_days_model_data(self, scopeid_list, mode, start, end, p=1, psize=20, order_field='user_visit'):
        excludes = ('id','mode','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' and scope_id in (%s)" % (start,end,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext).multi_col_groupby('scope_id','time')
            count = len(q)
            dlist = q[(p-1)*psize:p*psize]
        return {'count':count,'list':dlist} 

    def get_multi_days_model_data(self, scopeid_list, mode, start, end):
        excludes = ('id','mode','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        scope_list = ','.join([str(i) for i in scopeid_list])
        count,dlist = 0,[]
        if scope_list:
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' and scope_id in (%s)" % (start,end,scope_list)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext).multi_col_groupby('scope_id','time')
            count = len(q)
            dlist = q
        return {'count':count,'list':dlist}

    def get_multi_days_in_multi_month_proportion_stat(self, scope_id, mode, start, end, partner_id):
        q = Proportion.mgr().Q()
        q = q.filter(partner_id=partner_id)[0]
        proportion = q['proportion']
        excludes = ('id','mode','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        excludes_cols = ('id','mode','recharge','uptime','time','imei') 
        cols = [i for i in self._fields if i not in excludes_cols]
        
        delta = end - start
        days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
        basics = []
        if scope_id:
            for i in days:    
                dft = dict([(j,0) for j in BasicStat._fields])
                dft['time'] = i
                s = BasicStat.mgr().Q(time=i).filter(scope_id=scope_id,mode='day',time=i)[0] or dft
                s['title'] = i.strftime('%Y-%m-%d')
                basics.append(s)
            res = self.multiply_by_proportion_list(basics,proportion,cols)
        return res

    def get_multi_days_proportion_stat(self, scope_id, mode, start, end, partner_id):
        q = Proportion.mgr().Q()
        q = q.filter(partner_id=partner_id)[0]
        proportion = q['proportion']
        excludes = ('id','mode','recharge','uptime')
        sub = ','.join(['%s'%(i) for i in self._fields if i not in excludes])
        excludes_cols = ('id','mode','recharge','uptime','time','imei') 
        cols = [i for i in self._fields if i not in excludes_cols]
        if scope_id:
            qtype = 'SELECT %s'%sub
            ext = "time>='%s' AND time<'%s' and scope_id in (%s)" % (start,end,scope_id)
            q = self.Q(qtype=qtype,time=start).filter(mode=mode).extra(ext)
            res = self.multiply_by_proportion_list(q[:],proportion,cols)
        return res

    def multiply_by_proportion_list(self, inlist ,proportion, cols):
        if proportion:
            for basic in inlist:
                for col in cols:
                    if col in ('cfee','bfee'):
                        basic[col] = basic[col] * proportion
                    else:
                        basic[col] = int(basic[col] * proportion)
        return inlist

    def get_sum_new_user_run_by_scope_ids(self, scope_ids, start, mode='day'):
        sid = ','.join([str(scope_id['id']) for scope_id in scope_ids])
        qtype = 'SELECT SUM(new_user_run)new_user_run'
        ext = " scope_id IN ( %s )" % sid
        q = self.Q(qtype=qtype,time=start).filter(mode='day',time=start).extra(ext)[0]
        return q

class BasicStatv3(BasicStat):
    '''
    logstat_v3 basic stat model
    '''
    _db = 'logstatv3'

class VisitStat(Model):
    '''
    visit model
    '''
    _db = 'logstatV2'
    _table = 'visit_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','type','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month','year') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`type` varchar(64) NOT NULL DEFAULT ''", # page type:boy,girl,board...
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_type_time` (`scope_id`,`mode`,`type`,`time`)")
    
class TopNStat(Model):
    '''
    top N stat model
    '''
    _db = 'logstatV2'
    _table = 'topN_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','type','no','value','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`type` varchar(32) NOT NULL DEFAULT ''", # page type:tag,search,hotword
               "`no` int NOT NULL DEFAULT '0'",
               "`value` varchar(32) NOT NULL DEFAULT ''",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_type_no_time` (`scope_id`,`mode`,`type`,`no`,`time`)")

class BookStat(Model):
    '''
    book stat model
    '''
    _db = 'logstatV2'
    _table = 'book_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','book_id','charge_type','pv','uv','batch_pv',
                   'batch_uv','batch_fee','pay_down','free_down','pay_user','free_user','fee','real_fee','arpu','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`book_id` int NOT NULL DEFAULT '0'",
               "`charge_type` enum('book','chapter','other') NOT NULL DEFAULT 'book'",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`batch_pv` int NOT NULL DEFAULT '0'",
               "`batch_uv` int NOT NULL DEFAULT '0'",
               "`batch_fee` float NOT NULL DEFAULT '0'",
               "`pay_down` int NOT NULL DEFAULT '0'",
               "`free_down` int NOT NULL DEFAULT '0'",
               "`pay_user` int NOT NULL DEFAULT '0'",
               "`free_user` int NOT NULL DEFAULT '0'",
               "`fee` float NOT NULL DEFAULT '0'",
               "`real_fee` float NOT NULL DEFAULT '0'",
               "`arpu` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_time_type_book` (`scope_id`,`mode`,`time`,`charge_type`,`book_id`)")
    _fieldesc= {'id':'ID','scope_id':'scope_id','mode':'mode',
                'time':'time','book_id':'book_id','charge_type':'charge_type','pv':'简介访问数',
                'uv':'简介访问人数','batch_pv':'批量订购PV','batch_uv':'批量订购UV',
                'batch_fee':'批量订购阅饼消费','pay_down':'付费下载数','free_down':'免费下载数',
                'pay_user':'付费下载用户数','free_user':'免费下载用户数','fee':'阅饼消费','real_fee':'主账户阅饼消费',
                'arpu':'arpu','uptime':'更新时间'}

    def get_table(self, **kargs):
        dtime = kargs["time"]
        if isinstance(dtime,str):
            dtime = strptime(dtime, "%Y-%m-%d %H:%M:%S")
        return "%s_%d_%d" % (self._table,dtime.year,dtime.month)

    def init_table(self, year):
        for month in range(1,13):
            super(BookStat,self).init_table(time='%d-%d-01 00:00:00'%(year,month))

    def get_data(self, scope_id, mode, start, end, charge_type, bookid_list, page, psize):
        count,blist = 0,[]
        if bookid_list:
            while start < end:
                _end = min(time_next(start,'month'),end)
                #remove_bad_book_data = "(uv/5<(15*pay_user+1.7*free_user))"
                remove_bad_book_data = "(uv/10<(20*pay_user+1.7*free_user))"
                ext = "time>='%s' AND time<'%s' AND %s"%(start,_end,remove_bad_book_data)
                #ext = "time>='%s' AND time<'%s'"%(start,_end)
                q = self.Q(time=start).filter(scope_id=scope_id,mode=mode).extra(ext)
                charge_type and q.filter(charge_type=charge_type)
                bookid_list and q.extra("book_id in (%s)" % ','.join(bookid_list))
                count += q.count()
                blist.extend(q.data())
                start = _end
        return {'count':count,'list':blist}

    def get_data_by_multi_days(self, scope_id, mode, start, end, charge_type):
        res = {}
        if start <= end:
            _end = end - datetime.timedelta(days=1)
            qtype = "SELECT book_id,charge_type,'%s->%s' AS time,SUM(fee) AS fee,SUM(pay_down) AS pay_down,SUM(pay_user) AS pay_user,SUM(free_down) AS free_down,SUM(free_user) AS free_user,SUM(pv) AS pv,SUM(uv) AS uv,SUM(batch_fee) AS batch_fee,SUM(batch_pv) AS batch_pv,SUM(batch_uv) AS batch_uv" % (start,_end)
            #remove_bad_book_data = "(uv/5<(15*pay_user+1.7*free_user))"
            remove_bad_book_data = "(uv/10<(20*pay_user+1.7*free_user))"
            ext = "time>='%s' AND time<'%s' AND %s"%(start,end,remove_bad_book_data)
            q = self.Q(qtype=qtype,time=start).filter(scope_id=scope_id,mode=mode).extra(ext).groupby("book_id")
            charge_type and q.filter(charge_type=charge_type)
            res = q
        return res

    def get_accouting_multy_scope_stat(self, scopeid_list, order_field, start, end=None):
        scopeid = ','.join(str(i) for i in scopeid_list)
        remove_bad_book_data = "(uv/10<(20*pay_user+1.7*free_user))"
        if start and not end: 
            ext = "time='%s' AND %s"%(start,remove_bad_book_data)
        elif start and end: 
            ext = "time>='%s' AND time<'%s' AND %s"%(start,end,remove_bad_book_data)
        if scopeid_list:
            ext += 'AND scope_id in (%s)' % scopeid
        else:
            ext += 'AND scope_id in (%s)' % 0
        qtype = "SELECT book_id,charge_type,SUM(pv)pv,SUM(uv)uv,SUM(batch_pv)batch_pv,SUM(batch_uv)batch_uv,SUM(batch_fee)batch_fee,SUM(pay_down)pay_down,SUM(free_down)free_down,SUM(pay_user)pay_user,SUM(free_user)free_user,SUM(fee)fee,SUM(real_fee)real_fee"
        q = self.Q(qtype=qtype,time=start).extra(ext).multi_col_groupby('book_id','charge_type') 
        return q

class BookReferStat(Model):
    '''
    book refer stat model
    '''
    _db = 'logstatV2'
    _table = 'book_refer_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','p_key','book_id','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`p_key` varchar(64) NOT NULL DEFAULT ''",
               "`book_id` int NOT NULL DEFAULT '0'",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_time_pkey_bookid` (`scope_id`,`mode`,`time`,`p_key`,`book_id`)")
    def get_table(self, **kargs):
        dtime = kargs["time"]
        if isinstance(dtime,str):
            dtime = strptime(dtime, "%Y-%m-%d %H:%M:%S")
        return "%s_%d_%d" % (self._table,dtime.year,dtime.month)

    def init_table(self, year):
        for month in range(1,13):
            super(BookReferStat,self).init_table(time='%d-%d-01 00:00:00'%(year,month))

class BookTagStat(Model):
    '''
    book tag stat model
    '''
    _db = 'logstatV2'
    _table = 'book_tag_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','tag','pv','uv','pay_down','free_down',
                   'pay_user','free_user','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`tag` varchar(64) NOT NULL DEFAULT ''",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`pay_down` int NOT NULL DEFAULT '0'",
               "`free_down` int NOT NULL DEFAULT '0'",
               "`pay_user` int NOT NULL DEFAULT '0'",
               "`free_user` int NOT NULL DEFAULT '0'",
               "`fee` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_time_tag` (`scope_id`,`mode`,`time`,`tag`)")
    def get_table(self, **kargs):
        dtime = kargs["time"]
        if isinstance(dtime,str):
            dtime = strptime(dtime, "%Y-%m-%d %H:%M:%S")
        return "%s_%d_%d" % (self._table,dtime.year,dtime.month)

    def init_table(self, year):
        for month in range(1,13):
            super(BookTagStat,self).init_table(time='%d-%d-01 00:00:00'%(year,month))

class ProductStat(Model):
    '''
    product stat model : machine type stat
    '''
    _db = 'logstatV2'
    _table = 'product_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','type','product_name','count','ratio','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`type` enum('user','book','chapter') NOT NULL DEFAULT 'user'",
               "`product_name` varchar(64) NOT NULL DEFAULT ''",
               "`count` int NOT NULL DEFAULT '0'",
               "`ratio` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_productname_type_mode_startime` (`scope_id`,`product_name`,`type`,`mode`,`time`)")

class RechargeCenterStat(Model):
    '''
    Recharge center model
    '''
    _db = 'logstatV2'
    _table = 'recharge_center_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_time` (`scope_id`,`mode`,`time`)")

class RechargeTypeStat(Model):
    '''
    Recharge tyep model
    '''
    _db = 'logstatV2'
    _table = 'recharge_type_stat'
    _pk = 'id'
    _fields = set(['id','scope_id','mode','time','recharging_type','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`scope_id` int NOT NULL DEFAULT '0'",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`recharging_type` varchar(64) NOT NULL DEFAULT ''", # recharging type  1 alipay 2 SMS 3 prepaid_card 4 OPPO 5 UnionPay 
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `scopeid_mode_recharging_type_time` (`scope_id`,`mode`,`recharging_type`,`time`)")

class RechargeIncomeStat(Model):
    '''
    Recharge income model
    '''
    _db = 'logstatV2'
    _table = 'recharge_income_stat'
    _pk = 'id'
    _fields = set(['id','mode','time','recharging_type','income','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`recharging_type` varchar(64) NOT NULL DEFAULT ''", # recharging type  1 alipay 2 SMS 3 prepaid_card 4 OPPO 5 UnionPay 
               "`income` int NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `mode_recharging_type_time` (`mode`,`recharging_type`,`time`)")

class RechargeOrderSubmitStat(Model):
    '''
    Recharge order submit model
    '''
    _db = 'logstatV2'
    _table = 'recharge_order_submit_stat'
    _pk = 'id'
    _fields = set(['id','mode','time','pv','uv','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`recharging_type` varchar(64) NOT NULL DEFAULT ''", # recharge type 1-9
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `mode_recharging_type_time` (`mode`,`recharging_type`,`time`)")

class RechargeOrderSubmitSuccessStat(Model):
    '''
    Recharge order submit success model
    '''
    _db = 'logstatV2'
    _table = 'recharge_order_submit_success_stat'
    _pk = 'id'
    _fields = set(['id','mode','time','pv','uv','recharging_type','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`pv` int NOT NULL DEFAULT '0'",
               "`uv` int NOT NULL DEFAULT '0'",
               "`recharging_type` varchar(64) NOT NULL DEFAULT ''", # recharge type 1-9
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `mode_recharging_type_time` (`mode`,`recharging_type`,`time`)")

class RechargeDetailStat(Model):
    '''
    Recharge detail model
    '''
    _db = 'logstatV2'
    _table = 'recharge_detail_stat'
    _pk = 'id'
    _fields = set(['id','mode','time','recharging_type','price','recharge_times','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
                "`mode` enum('hour','day','week','month') NOT NULL DEFAULT 'day'",
                "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
                "`recharging_type` varchar(64) NOT NULL DEFAULT ''",
                "`price` int NOT NULL DEFAULT '0'",
                "`recharge_times` int NOT NULL DEFAULT '0'",
                "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                "PRIMARY KEY `idx_id` (`id`)",
                "UNIQUE KEY `mode_recharging_type_time_price` (`mode`,`recharging_type`,`time`,`price`)")

class FactorySumStat(Model):
    '''
    factory sum model
    '''
    _db = 'logstatV2'
    _table = 'factory_sum_stat'
    _pk = 'id'
    _fields = set(['id','time','factory_id','visits','imei','user_run','new_user_run','user_visit',
        'new_user_visit','active_user_visit','user_retention','pay_user','cpay_down','cfree_down','bpay_down',
        'bfree_down','cpay_user','cfree_user','bpay_user','bfree_user','cfee','bfee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`factory_id` int(11) NOT NULL DEFAULT '0'",
               "`visits` int NOT NULL DEFAULT '0'",
               "`imei` int NOT NULL DEFAULT '0'",
               "`user_run` int NOT NULL DEFAULT '0'",
               "`new_user_run` int NOT NULL DEFAULT '0'",
               "`user_visit` int NOT NULL DEFAULT '0'",
               "`new_user_visit` int NOT NULL DEFAULT '0'",
               "`active_user_visit` int NOT NULL DEFAULT '0'",
               "`user_retention` int NOT NULL DEFAULT '0'",
               "`pay_user` int NOT NULL DEFAULT '0'",
               "`cpay_down` int NOT NULL DEFAULT '0'",
               "`cfree_down` int NOT NULL DEFAULT '0'",
               "`bpay_down` int NOT NULL DEFAULT '0'",
               "`bfree_down` int NOT NULL DEFAULT '0'",
               "`cpay_user` int NOT NULL DEFAULT '0'",
               "`cfree_user` int NOT NULL DEFAULT '0'",
               "`bpay_user` int NOT NULL DEFAULT '0'",
               "`bfree_user` int NOT NULL DEFAULT '0'",
               "`cfee` float NOT NULL DEFAULT '0'",
               "`bfee` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `time_factory_id` (`time`,`factory_id`)")
    _fieldesc= {'id':'ID','time':'时间','visits':'PV',
                'imei':'IMEI数','user_run':'启动用户','new_user_run':'新增启动用户','user_visit':'访问用户',
                'new_user_visit':'新增访问用户','active_user_visit':'活跃用户','user_retention':'留存用户',
                'pay_user':'付费用户','cpay_down':'按章付费下载','cfree_down':'按章免费下载',
                'bpay_down':'按本付费下载','bfree_down':'按本免费下载','cpay_user':'按章付费用户',
                'cfree_user':'按章免费用户','bpay_user':'按本付费用户','bfree_user':'按本免费用户',
                'cfee':'按章月饼消费','bfee':'按本月饼消费','uptime':'更新时间'}

class JobStatus(Model):
    '''
    when 
    '''
    _db = 'logstatV2'
    _table = 'job_status'
    _pk = 'id'
    _fields = set(['id','time','status','creatime','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`status` enum('start','running','completed') NOT NULL DEFAULT 'start'",
               "`creatime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `time` (`time`)")

class PushStat(Model):

    _db = 'logstatV2'
    _table = 'push_stat'
    _pk = 'id'
    _fields = set(['id','ds','user_push','user_click', 'user_consume', 'user_download'])

    def get_push_stat(self,start,end):
        start = start.strftime('%Y%m%d')
        end = end.strftime('%Y%m%d')
        excludes = ('id')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds<='%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class BkrackMsg(Model):
    _db = 'logstatV2'
    _table = 'bkrack_msg'
    _pk = 'id'
    _fields = set(['id','type','msg_title','msg_subtitle','msg_desc','msg_url','pic_url','icon','book_ids','version_id','version_name',
            'channel_id','phonemodel_name','phonemodel_id','end_time','action','hidden','create_user','create_time','update_user',
            'update_time','status','is_push','start_time'])

    def get_all_stat(self,start,end):
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        excludes = ('update_time')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "start_time>='%s' and start_time<='%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext).orderby('start_time','DESC')
        return q

    def get_all_stat_multi_pids(self,start,end,pids):
        pid = ','.join([str(i) for i in pids])
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        excludes = ('update_time')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "start_time>='%s' and start_time<='%s' and id in (%s)" % (start,end,pid)
        q = self.Q(qtype=qtype,time=start).extra(ext).orderby('start_time','DESC')
        return q


class QueryTimeCount(Model):
    _db = 'logstatV2'
    _table = 'query_time_count'
    _pk = 'id'
    _fields = set(['id','user','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`user` varchar(64) NOT NULL DEFAULT ''",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)")
    
    def get_user_today_query_count(self,user,time):
        qtype = "SELECT *"
        ext = "uptime>='%s'"%time
        q = self.Q(qtype=qtype,time=time).filter(user=user).extra(ext)
        return q.count()

class PustStat(Model):
    _db = 'logstatV2'
    _table = 'pust_stat'
    _pk = 'id'
    _fields = set(['id','pid','channel','innerVer','visits','user_visit','pv','uv','ds'])

    def get_all_stat(self,start,end):
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        excludes = ('id')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <='%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

    def get_daily_stat(self,start,end,is_pid_null=True):
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        if is_pid_null:
            opt = '='
        else:
            opt = '!='
        sql = "SELECT ds,SUM(user_visit)user_visit,SUM(pv)pv,SUM(uv)uv FROM pust_stat WHERE pid %s'' and ds>='%s' and ds<='%s' GROUP BY ds ORDER BY ds" % (opt,start,end)
        q = self.raw(sql)
        return q 

    def get_daily_sum_stat(self,start,end):
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        sql = "SELECT ds,SUM(user_visit)user_visit,SUM(pv)pv,SUM(uv)uv FROM pust_stat WHERE ds>='%s' and ds<='%s' GROUP BY ds ORDER BY ds" % (start,end)
        q = self.raw(sql)
        return q 

    def get_push_user(self,time,pid):
        sql = "SELECT SUM(user_visit)user_visit FROM pust_stat WHERE pid =%s AND ds = '%s'" % (pid,time)
        q = self.raw(sql)
        return q

    def get_pid(self,stats):
        for stat in stats:
            stat['pids'] = self.mgr().get_pid_from_ds(stat['ds'])
        return stats
    
    def get_pid_from_ds(self,ds):
        sql = "SELECT DISTINCT(pid) FROM pust_stat WHERE ds='%s' AND pid<>''" % (ds)
        q = self.raw(sql)[:]
        pids = []
        for i in q:
            if ',' not in i['pid']:
                pids.append(i['pid'])
            else:
                tmp = i['pid'].split(',')
                for j in tmp:
                    pids.append(j)

        pids = list(set(pids))
        pids = ','.join(str(pid) for pid in pids)
        return pids

class Qiandao(Model):
    _db = 'logstatV2'
    _table = 'qiandao_level_detail'
    _fields = set(['level','card6','card11','card16','card21','card26','card52','ds'])

    def get_all_stat(self,start,end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class QiandaoBasic(Model):
    _db = 'logstatV2'
    _table = 'qiandao_basic_detail'
    _fields = set(['qiandaonum','buqiannum1','buqiannum2','bangdingnum','mergenum','ds'])

    def get_all_stat(self,start,end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class QiandaoRecharge(Model):
    _db = 'logstatV2'
    _table = 'qiandao_recharge_detail'
    _fields = set(['num','rechargetype','amount','ds'])

    def get_all_stat(self,start,end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class OperaBaoyueSum(Model):
    _db = 'logstatV2'
    _table = 'opera_baoyue_sum'
    _fields = set(['ds','partner_id','innerver','versionname','orderpv','orderuv','renewtimes','rechargingnum','giftrechargingnum'])

    def get_all_stat(self, start, end):
        start = start.strftime('%Y%m%d')
        end = end.strftime('%Y%m%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

    def get_one_day_stat(self, time):
        time = time.strftime('%Y%m%d')
        sql = 'SELECT ds,sum(orderpv) orderpv,sum(orderuv) orderuv,sum(renewtimes) renewtimes, sum(rechargingnum) rechargingnum, sum(giftrechargingnum) giftrechargingnum FROM opera_baoyue_sum where ds=%s' % (time)
        q = self.raw(sql)
        return q

class BookCategory(Model):
    _db = 'logstatV2'
    _table = 'book_category'
    _fields = set(['id','cate0','cate1','cate2','uptime']) 
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`cate0` varchar(64) NOT NULL DEFAULT ''",
        "`cate1` varchar(64) NOT NULL DEFAULT ''",
        "`cate2` varchar(64) NOT NULL DEFAULT ''",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `cate2` (`cate2`)")

class OperaBaoyueBook(Model):
    _db = 'logstatV2'
    _table = 'opera_baoyue_book'
    _fields = set(['ds','bookid','briefpv','briefuv','downpv','downuv'])

    def get_one_day_stat(self,time,book_id=''):
        time = time.strftime('%Y-%m-%d')
        if book_id == '':
            sql = "SELECT * FROM opera_baoyue_book WHERE ds='%s'" % (time)
        else:
            book_id = int(book_id)
            sql = "SELECT * FROM opera_baoyue_book WHERE ds='%s' and bookid in (%s)" % (time,book_id)
        q = self.raw(sql)
        return q

    def get_stat(self, start, end, book_id=''):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "ds>='%s' and ds <='%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        if book_id != '':
            q = q.filter(bookid=book_id)
        return q

class TxtRecommendation(Model):
    _db = 'logstatV2'
    _table = 'txt_recommendation'
    _fields = set(['id','time','book_id','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`book_id` int NOT NULL DEFAULT '0'",
        "`pv` int NOT NULL DEFAULT '0'",
        "`uv` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)", 
        "UNIQUE KEY `time_bookid` (`time`,`book_id`)")   

    def get_stat(self, start, end, order_field, book_id=''):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        ext = "time>='%s' and time <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        if book_id != '':
            q = q.filter(book_id=book_id)
        if order_field == '':
            q = q.orderby('time','ASC')
        else:
            q = q.orderby(order_field,'DESC')
        return q

class SyncThrdPartnerbyplan(Model):
    _db = 'logstatV2'
    _table = 'sync_thrd_partnerbyplan'
    _fields = set(['id','plan_id','partner_id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
    "`plan_id` int NOT NULL DEFAULT '0'",
    "`partner_id` int NOT NULL DEFAULT '0'",
    "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "PRIMARY KEY `idx_id` (`id`)",
    "UNIQUE KEY `plan_id_partner_id` (`plan_id`,`partner_id`)")

    def get_partner_list_by_plan_id(self,plan_id):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        if plan_id:
            q = q.filter(plan_id=plan_id)
        return q

class ArpuOneWeekFee(Model):
    _db = 'logstatV2'
    _table = 'arpu_one_week_fee'
    _fields = set(['id','time','one_week_fee','new_user_visit','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`one_week_fee` double NOT NULL DEFAULT '0'",
        "`new_user_visit` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")

    def get_stat_by_time(self,time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time).filter(time=time)
        return q

class TemparpuOneWeekNewUserVisit(Model):
    _db = 'logstatV2'
    _table = 'temp_arpu_one_week_new_user_visit'
    _fields = set(['id','time','new_user_visit','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`new_user_visit` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")

class Arpu7DaysArpuStat(Model):
    _db = 'logstatV2'
    _table = 'arpu_7days_arpu_stat'
    _fields = set(['one_week_fee','new_user_visit','time'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`one_week_fee` double NOT NULL DEFAULT '0'",
        "`new_user_visit` int NOT NULL DEFAULT '0'",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")
    
    def get_arpu_7days_stat(self, time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time).filter(time=time)
        return q

class Arpu30DaysArpuFeeStat(Model):
    _db = 'logstatV2'
    _table = 'arpu_30_days_arpu_fee_stat'
    _fields = set(['id','thirty_days_fee','time','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`thirty_days_fee` double NOT NULL DEFAULT '0.0'",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")

    def get_arpu_30_days_stat(self, time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time).filter(time=time)
        return q

class Arpu90DaysArpuFeeStat(Model):
    _db = 'logstatV2'
    _table = 'arpu_90_days_arpu_fee_stat'
    _fields = set(['id','ninety_days_fee','time','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`ninety_days_fee` double NOT NULL DEFAULT '0.0'",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")
 
    def get_arpu_90_days_stat(self, time):
        excludes = ('id','uptime')
        sub = ','.join([i for i in self._fields if i not in excludes])
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype,time=time).filter(time=time)
        return q

class OperateCenter(Model):
    _db = 'logstatV2'
    _table = 't_dialog_info'
    _fields = set(['dateid','type','bookid','pv','uv'])

    def get_operate_center_stat(self, start, end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields])
        qtype = 'SELECT %s' % sub
        ext = "dateid>='%s' and dateid <= '%s'" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext)
        return q

class MonthlyBusinessStat(Model):
    _db = 'logstatV2'
    _table = 'monthly_business_stat'
    _fields = set(['id','time','partner_id','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
    "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
    "`partner_id` int NOT NULL DEFAULT '0'",
    "`fee` float NOT NULL DEFAULT '0'",
    "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "PRIMARY KEY `idx_id` (`id`)",
    "UNIQUE KEY `time_partner_id` (`time`,`partner_id`)")

    def get_monthly_busiess_stat(self, time, partner_list):
        if partner_list:
            partners = ','.join([str(i.partner_id) for i in partner_list])
            qtype = 'SELECT sum(fee)fee'
            ext = "time='%s' AND partner_id in (%s)"%(time,partners)
            q = self.Q(qtype=qtype,time=time).extra(ext)        
            return q

class MonthlyBusinessStatDaily(Model):
    _db = 'logstatV2'
    _table = 'monthly_business_stat_daily'
    _fields = set(['id','time','partner_id','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
    "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
    "`partner_id` int NOT NULL DEFAULT '0'",
    "`fee` float NOT NULL DEFAULT '0'",
    "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "PRIMARY KEY `idx_id` (`id`)",
    "UNIQUE KEY `time_partner_id` (`time`,`partner_id`)")

    def get_monthly_busiess_stat(self, time ,partner_list):
        if partner_list:
            partners = ','.join([str(i.partner_id) for i in partner_list])
            qtype = 'SELECT sum(fee)fee'
            ext = "time like '%s%%' AND partner_id in (%s)"%(time,partners)
            q = self.Q(qtype=qtype,time=time).extra(ext)
            return q

    def get_monthly_busiess_stat_multy_days(self, time, next_month_start, partner_list):
        if partner_list:
            partners = ','.join([str(i.partner_id) for i in partner_list])
            qtype = 'SELECT sum(fee)fee'
            ext = "time >= '%s' AND time <'%s' AND partner_id in (%s)"%(time,next_month_start,partners)
            q = self.Q(qtype=qtype,time=time).extra(ext)
            return q

    def get_monthly_busiess_stat_multy_days_proportion(self, time, next_month_start, partner_list):
        res = {'fee':0}
        if partner_list:
            for partner in partner_list:
                qtype = 'SELECT %s*SUM(fee)fee'%partner['proportion']
                ext = "time >= '%s' AND time <'%s' AND partner_id =%s"%(time,next_month_start,partner['partner_id'])
                q = self.Q(qtype=qtype,time=time).extra(ext)
                if q[0]['fee']:
                    res['fee'] += int(q[0]['fee'])
        return res

class AccountingFactoryStart(Model):
    _db = 'logstatV2'
    _table = 'accounting_factory_start'
    _pk = 'id'
    _fields = set(['id','time','factory_id','coefficient','author','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
    "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
    "`factory_id` int NOT NULL DEFAULT '0'",
    "`coefficient` float NOT NULL DEFAULT '1.0'",
    "`author` varchar(32) NOT NULL DEFAULT ''",  
    "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "PRIMARY KEY `idx_id` (`id`)",
    "UNIQUE KEY `time_factory_id` (`time`,`factory_id`)")

    def get_partner_start_time(self,factory_id):
        if partner_id:
            qtype = 'SELECT time'
            q = self.Q(qtype=qtyep)
            q = q.filter(factory_id=factory_id)
            return q

    def get_coefficient_by_partner_id(self,partner_id):
        if partner_id:
            factory_id = Partner.mgr().Q().filter(partner_id=partner_id)[0]['factory_id']
            if factory_id:
                q = self.Q().filter(factory_id=factory_id)
                return q

class IOSPartner(Model):
    _db = 'logstatV2'
    _table = 'ios_partner'
    _pk = 'id'
    _fields = set(['id','partner_id','type','status','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`partner_id` int NOT NULL DEFAULT '0'",
        "`type` varchar(32) NOT NULL DEFAULT ''",  
        "`status` enum('valid','invalid') NOT NULL DEFAULT 'valid'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `partner_id` (`partner_id`)")

class VPCustom(Model):
    _db = 'dw_v6_test'
    _table = 'vp_custom'
    _fields = set(['ds','hour','productid','name','customtimes','customusers'])

    def get_vp_stat(self, start, end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields if i not in ('hour')])
        qtype = "SELECT ds,name,productid,SUM(customtimes)customtimes,SUM(customusers)customusers"
        ext = "ds>='%s' and ds <= '%s' and `name` != ''" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext).thrid_col_groupby('ds','name','productid')
        return q

class VPCancel(Model):
    _db = 'dw_v6_test'
    _table = 'vp_cancel'
    _fields = set(['ds','hour','productid','name','canceltimes','cancelusers'])

    def get_vp_stat(self, start, end):
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        sub = ','.join([i for i in self._fields if i not in ('hour')])
        qtype = "SELECT ds,name,productid,SUM(canceltimes)canceltimes,SUM(cancelusers)cancelusers"
        ext = "ds>='%s' and ds <= '%s' and `name` != ''" % (start,end)
        q = self.Q(qtype=qtype,time=start).extra(ext).thrid_col_groupby('ds','name','productid')
        return q

class ReserveBook(Model):
    _db = 'external'
    _table = 'reserve_book'
    _fields = set(['id','datetime','tds','num'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`datetime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`tds` varchar(32) NOT NULL DEFAULT ''",
        "`num` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `datetime_tds` (`datetime`,`tds`)")

class Partnerv2(Partner):
    '''
    logstatV2.partner_v2 model
    '''
    _table = 'partner_v2'
    _fields = set(['id','partner_id','factory_id','proportion','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`partner_id` int(11) NOT NULL",
        "`factory_id` int(11) NOT NULL",
        "`proportion` float NOT NULL DEFAULT 1.0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `partner_id_factory_id` (`partner_id`,`factory_id`)")

class WechatStat(Model):
    '''
    logstatV2.wechat_stat
    '''
    _db = 'logstatV2'
    _table = 'wechat_stat'
    _fields = set(['id','datetime','bookid','pv','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`datetime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`bookid` int NOT NULL DEFAULT '0'",
        "`pv` int NOT NULL DEFAULT '0'",
        "`uv` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `datetime_bookid` (`datetime`,`bookid`)")

class RetentionStat(Model):
    '''
    datamining.new_user_run_retention_week
    '''
    _db = 'datamining'
    _table = 'new_user_run_retention_week'
    _fields = set(['id','start_time','week_num','new_user_run','retention','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`start_time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`week_num` int NOT NULL DEFAULT '0'",
        "`new_user_run` int NOT NULL DEFAULT '0'",
        "`retention` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `start_time_` (`start_time`)")

class UserBandV6(Model):
    '''
    userbandv6
    '''
    _db = 'logstatV2'
    _table = 'UserBandV6'
    _fields = set(['id','datetime','username','type','uid','plan_id','inner_version','partner','productname','amount','ds','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`datetime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`username` varchar(32) NOT NULL DEFAULT ''",
        "`type` varchar(32) NOT NULL DEFAULT ''",
        "`uid` varchar(128) NOT NULL DEFAULT ''",
        "`plan_id` int NOT NULL DEFAULT '0'",
        "`inner_version` varchar(32) NOT NULL DEFAULT ''",
        "`partner` int NOT NULL DEFAULT '0'",
        "`productname` varchar(128) NOT NULL DEFAULT ''",
        "`amount` int NOT NULL DEFAULT '0'",
        "`ds` varchar(32) NOT NULL DEFAULT ''",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `id_` (`id`)")

class FuJin(Model):
    '''
    FuJin
    '''
    _db = 'logstatV2'
    _table = 'fujin'
    _fields = set(['id','time','pv','uv','user_pay','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`pv` int NOT NULL DEFAULT '0'",
        "`uv` int NOT NULL DEFAULT '0'",
        "`user_pay` int NOT NULL DEFAULT '0'",
        "`fee` float NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_` (`time`)")

class RunUserProvince(Model):
    '''
    run_user_province
    '''
    _db = 'logstatV2'
    _table = 'province_run_user'
    _fields = set(['id','time','version_name','partner_id','province','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`version_name` varchar(32) NOT NULL DEFAULT ''", 
        "`partner_id` int NOT NULL DEFAULT '0'",
        "`province` varchar(32) NOT NULL DEFAULT ''", 
        "`uv` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_version_name_partner_id_province` (`time`,`version_name`,`partner_id`,`province`)")

class VisUserProvince(Model):
    '''
    vis_vser_province
    '''
    _db = 'logstatV2'
    _table = 'province_visit_vser'
    _fields = set(['id','time','version_name','partner_id','province','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`version_name` varchar(32) NOT NULL DEFAULT ''", 
        "`partner_id` int NOT NULL DEFAULT '0'",
        "`province` varchar(32) NOT NULL DEFAULT ''", 
        "`uv` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `time_version_name_partner_id_province` (`time`,`version_name`,`partner_id`,`province`)")

class UserTask(Model):
    '''
    user task
    '''
    _db = 'logstatV2'
    _table = 'user_task'
    _fields = set(['id','time','task_type','task_id','task_name','uv','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`task_type` varchar(32) NOT NULL DEFAULT ''", 
        "`task_id` int NOT NULL DEFAULT '0'",
        "`task_name` varchar(32) NOT NULL DEFAULT ''", 
        "`uv` int NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `user_task_time_task_type_task_id` (`time`,`task_type`,`task_id`)")

class PopWindowStat(Model):
    '''
    pop window 2
    '''
    _db = 'logstatV2'
    _table = 'pop_window_stat'
    _fields = set(['id','time','book_id','type','request_user','cd_user','pay_user','real_pay_user','fee','accounting_fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`book_id` int NOT NULL DEFAULT '0'",
        "`type` varchar(32) NOT NULL DEFAULT ''", 
        "`request_user` int NOT NULL DEFAULT '0'",
        "`cd_user` int NOT NULL DEFAULT '0'",
        "`pay_user` int NOT NULL DEFAULT '0'",
        "`real_pay_user` int NOT NULL DEFAULT '0'",
        "`fee` float NOT NULL DEFAULT '0'",
        "`accounting_fee` float NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `pop_window_2_time_book_id_type` (`time`,`book_id`,`type`)")

class IncomeByPk(Model):
    '''
    datamining.income by pk
    '''
    _db = 'datamining'
    _table = 'income_by_pk'
    _fields = set(['id','time','pkname','uv','real_amount','amount','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`pkname` varchar(32) NOT NULL DEFAULT ''", 
        "`uv` int NOT NULL DEFAULT '0'",
        "`real_amount` float NOT NULL DEFAULT '0'",
        "`amount` float NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `income_by_pk_time_pkname` (`time`,`pkname`)")

class TmpUserTask(Model):
    '''
    datamining.tmp_user_task
    '''
    _db = 'datamining'
    _table = 'tmp_user_task'
    _fields = set(['id','time','user_name','action_type','task_id','task_name','ds','amount','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`user_name` varchar(128) NOT NULL DEFAULT ''",
        "`action_type` varchar(128) NOT NULL DEFAULT ''",
        "`task_id` varchar(32) NOT NULL DEFAULT ''",
        "`task_name` varchar(128) NOT NULL DEFAULT ''",
        "`ds` varchar(32) NOT NULL DEFAULT ''",
        "`amount` float NOT NULL DEFAULT '0'",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `id` (`id`)")

class BookWorm(Model):
    '''
    bookworm
    '''
    _db = 'logstatV2'
    _table = 'bookworm'
    _fields = set(['id','time','pv','uv','right_user','right_num','wrong_user','wrong_num','pay_user','pay_times','recharge_page_pv','recharge_page_uv','recharge_user','amount','pay_num1','pay_num2','pay_num3','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`pv` int NOT NULL DEFAULT 0",
        "`uv` int NOT NULL DEFAULT 0",
        "`right_user` int NOT NULL DEFAULT 0",
        "`right_num` int NOT NULL DEFAULT 0",
        "`wrong_user` int NOT NULL DEFAULT 0",
        "`wrong_num` int NOT NULL DEFAULT 0",
        "`pay_user` int NOT NULL DEFAULT 0",
        "`pay_times` int NOT NULL DEFAULT 0",
        "`recharge_page_pv` int NOT NULL DEFAULT 0",
        "`recharge_page_uv` int NOT NULL DEFAULT 0",
        "`recharge_user` int NOT NULL DEFAULT 0",
        "`amount` float NOT NULL DEFAULT '0'",
        "`pay_num1` int NOT NULL DEFAULT 0",
        "`pay_num2` int NOT NULL DEFAULT 0",
        "`pay_num3` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `id` (`id`)")

class PaymentRechargeDailyStat(Model):
    '''
    payment_recharge_daily_stat
    '''
    _db = 'logstatV2'
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
    _db = 'logstatV2'
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
    _db = 'logstatV2'
    _table = 'payment_recharging_detail_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','curbustype','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`curbustype` varchar(128) NOT NULL DEFAULT ''",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_stat_tm_or_pa_id_in_cu` (`time`,`origin`,`partner_id`,`innerver`,`curbustype`)")

class PaymentRechargingDetailOrderStat(Model):
    '''
    payment_recharging_detail_order_stat
    '''
    _db = 'logstatV2'
    _table = 'payment_recharging_detail_order_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_stat_tm_or_pa_id_in` (`time`,`origin`,`partner_id`,`innerver`)")

class PaymentRechargingDetailFinishStat(Model):
    '''
    payment_recharging_detail_order_stat
    '''
    _db = 'logstatV2'
    _table = 'payment_recharging_detail_finish_stat'
    _fields = set(['id','time','uv','pv','origin','partner_id','innerver','amount','gift_amount','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
        "`uv` int NOT NULL DEFAULT 0",
        "`pv` int NOT NULL DEFAULT 0",
        "`origin` varchar(32) NOT NULL DEFAULT ''",
        "`partner_id` int NOT NULL DEFAULT 0",
        "`innerver` int NOT NULL DEFAULT 0",
        "`amount` float NOT NULL DEFAULT 0",
        "`gift_amount` float NOT NULL DEFAULT 0",
        "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "PRIMARY KEY `idx_id` (`id`)",
        "UNIQUE KEY `payment_recharging_detail_stat_tm_or_pa_id_in` (`time`,`origin`,`partner_id`,`innerver`)")

class TmpFeeRealFeeStat(Model):
    '''
    tmp_fee_realfee_stat
    '''
    _db = 'logstatV2'
    _table = 'tmp_fee_realfee_stat'
    _fields = set(['time','run_id','book_id','fee','real_fee','gift_fee'])
    _scheme = ("`run_id` int NOT NULL DEFAULT 0",
        "`book_id` int NOT NULL DEFAULT 0",
        "`fee` float NOT NULL DEFAULT 0",
        "`real_fee` float NOT NULL DEFAULT 0",
        "`gift_fee` float NOT NULL DEFAULT 0",
        "`time` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'")

