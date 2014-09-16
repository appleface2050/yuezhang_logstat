#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat,Partnerv2
from model.factory import Factory,Partner
from model.stat import FactorySumStat,QueryTimeCount,MonthlyBusinessStat,AccountingFactoryStart,MonthlyBusinessStatDaily
from service import Service
from lib.excel import Excel
from conf.settings import Query_Limit_Count,MONTHLY,SAFE_USER,SPECIAL_FACTORY_COEFFICIENT,SPECIAL_FACTORY_COEFFICIENT2
from calendar import monthrange
from conf.settings import ACCOUNTING_FACTORY_TITLE

class AccountingHandler(BaseHandler):
    def index(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        factory_id = int(self.get_argument('factory_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        query_mode = self.get_argument('query_mode','')
        group = self.get_argument('group','')
        order_field = self.get_argument('orderField','user_run')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = tody - datetime.timedelta(days=30)
        # factory
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],query_mode)
        #for safety reason, some time a perm doesn't have a val, and in this Situation a factory CANN'T see all factory accouting data!!!
        cuser = self.get_current_user() 
        if len(factory_list)>150 and (cuser['name'] not in SAFE_USER):
            factory_list = []
        if factory_id == 0 and factory_list:
            factory_id = factory_list[0].id
        partner_list = Partnerv2.mgr().Q().filter(factory_id=factory_id)[:]
        # scope list
        scopeid_list = []
        for i in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=i.partner_id,version_name=version_name,product_name=product_name)[0]
            if scope:
                scopeid_list.append(scope.id)
    
    def factorydaily(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        factory_id = int(self.get_argument('factory_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        query_mode = self.get_argument('query_mode','accounting_basic')
        group = self.get_argument('group','')
        order_field = self.get_argument('orderField','user_run')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        date = self.get_argument('date','')
        action = self.get_argument('action','')
        
        # Special factory
        special_factory_list = SPECIAL_FACTORY_COEFFICIENT.keys()
        special_factory_list2 = SPECIAL_FACTORY_COEFFICIENT2.keys()

        # factory
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],query_mode)
        #for safety reason, some time a perm doesn't have a val, and in this Situation a factory CANN'T see all factory accouting data!!!
        cuser = self.get_current_user()
        if len(factory_list)>150 and (not cuser['is_staff']):
            factory_list = []
        if factory_id == 0 and factory_list:
            factory_id = factory_list[0].id
        partner_list = Partnerv2.mgr().Q().filter(factory_id=factory_id)[:]
        #get facotry start time
        if partner_list:
            q_start = AccountingFactoryStart.mgr().Q().filter(factory_id=partner_list[0]['factory_id'])
        else:
            q_start = None
        start_time = None
        user = self.get_current_user()
        if user['name'] in SAFE_USER:
            start_time = datetime.datetime(2012,12,31)
        coefficient = 1.0
        if q_start:
            start_time = q_start[0]['time']
            coefficient = q_start[0]['coefficient']
        # scope list
        scopeid_list = []
        scope_proportion_list = []
        for i in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=i.partner_id,version_name=version_name,product_name=product_name)[0]
            if scope:
                scopeid_list.append(scope.id)
                temp = {'proportion':i['proportion'],'scope_id':scope.id,'partner_id':i['partner_id']}
                scope_proportion_list.append(temp)
        # basic stat
        basics = []
        dft = dict([(i,0) for i in BasicStat._fields])
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d') 
        _start,days = start,[]
        if start_time:
            if start_time > _start: #限定开始时间
                _start = start_time
        else:
            _start = datetime.datetime(3012,01,01) # 没配置的不现实
        while _start < tody:
            _end = _start + datetime.timedelta(days=1)
            #basic = BasicStat.mgr().get_data_by_multi_scope(scopeid_list,mode='day',start=_start,end=_end)
            basic = BasicStat.mgr().get_data_by_multi_scope_proportion(scope_proportion_list,mode='day',start=_start,end=_end)
            #Special factory Coefficient 
            if factory_id in special_factory_list:
                statttime = SPECIAL_FACTORY_COEFFICIENT[factory_id]['starttime']
                sfc = SPECIAL_FACTORY_COEFFICIENT[factory_id]['coefficient']
                if _start.strftime('%Y-%m-%d')>= statttime:
                    for i in basic.keys():  #乘以系数
                        basic[i] = float(int(basic[i])) * coefficient #要和商务月报数据保持一致，所以这么处理
                        basic[i] = "%.01f" % basic[i]
                        basic[i] = float(basic[i]) * sfc
                        basic[i] = int(basic[i])
                else:
                    for i in basic.keys():  #乘以系数
                        basic[i] = float(basic[i]) * coefficient
                        basic[i] = long(basic[i])
            #Special factory Coefficient2
            if factory_id in special_factory_list2:
                statttime2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['starttime']
                sfc2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['coefficient']
                if _start.strftime('%Y-%m-%d')>= statttime2:
                    for i in basic.keys():  #乘以系数
                        basic[i] = float(basic[i]) * sfc2
                        basic[i] = int(basic[i])
            
            basic['title'] = _start.strftime('%Y-%m-%d')
            basics.append(basic)
            days.append(_start)
            _start = _end
        # for chart
        x_axis = ['%s'%i.strftime('%m-%d') for i in days]
        results = {}
        excludes = ('id','scope_id','mode','time','bfee','cfee','recharge','uptime','batch_fee','batch_pv','batch_uv','visits','user_visit',
                    'new_user_visit','active_user_visit','imei','user_retention')
        for k in [i for i in BasicStat._fields if i not in excludes]:
            results[k] ={'title':BasicStat._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in basics])}
        for i in basics:
            i['feesum'] = float(i['consumefee'])+i['msgfee']
            i['feesum'] = int(i['feesum'])
        acc_stats = {}
        for basic in basics:
            for field in basic:
                if field != 'title':
                    basic[field] = int(basic[field])
                    acc_stats[field] = basic[field] + acc_stats.get(field,0)
        acc_stats['title'] = '总计'
        if basics:
            basics.append(acc_stats)
        acc = {'title':'总计'}
        if action == 'export':  
            excel_title = [('title','时间'),('user_run','启动用户'),('new_user_run','新增启动用户'),
            ('pay_user','付费用户'),
            ('cpay_down','章付费数'),('bpay_down','本付费数'),
            ('cpay_user','章付费用户'),('bpay_user','本付费用户'),
            ('cfree_down','章免费数'),('bfree_down','本免费数'),
            ('cfree_user','章免费用户'),('bfree_user','本免费用户'),
            ('feesum','收入')]
            xls = Excel().generate(excel_title,basics,1) 
            filename = 'accounting_factorydaily_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/accounting_factorydaily.html',
                    latform_id=platform_id,run_id=run_id,plan_id=plan_id,factory_id=factory_id,tpl_titles=ACCOUNTING_FACTORY_TITLE,
                    version_name=version_name,product_name=product_name,query_mode=query_mode,
                    basics=basics,start=start.strftime('%Y-%m-%d'),date=yest.strftime('%Y-%m-%d'),
                    factory_list=factory_list,x_axis=x_axis,results=results)

    def productdaily(self):
        order_field = self.get_argument('orderField','user_run')
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        group = self.get_argument('group','')
        psize = int(self.get_argument('numPerPage',20))
        start = self.get_argument('start','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        action = self.get_argument('action','')
        factory_id = int(self.get_argument('factory_id',0))
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        query_mode = self.get_argument('query_mode','accounting_product') 
        if not start:
            start = yest
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        # Special factory
        special_factory_list = SPECIAL_FACTORY_COEFFICIENT.keys()
        special_factory_list2 = SPECIAL_FACTORY_COEFFICIENT2.keys()

        # factory
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],'accounting_product')
        #for safety reason, some time a perm doesn't have a val, and in this Situation a factory CANN'T see all factory accouting data!!!
        cuser = self.get_current_user()
        if len(factory_list)>150 and (not cuser['is_staff']):
            factory_list = []
        if factory_id == 0 and factory_list:
            factory_id = factory_list[0].id
        partner_list = Partnerv2.mgr().Q().filter(factory_id=factory_id)[:]
        #get facotry start time
        if partner_list:
            q_start = AccountingFactoryStart.mgr().Q().filter(factory_id=partner_list[0]['factory_id'])
        else:
            q_start = None
        coefficient = 1.0
        if q_start:
            start_time = q_start[0]['time']
            coefficient = q_start[0]['coefficient']
        else:
            start_time = datetime.datetime(2014,12,31) 
            user = self.get_current_user()
            if user['name'] in SAFE_USER:
                start_time = datetime.datetime(2012,12,31)
            coefficient = 1.0

        if start_time:
            if start_time > start:
                start = start_time
        
        scopeid_product,model_list = {},[]
        if factory_id == -1:
            #partnerid_list = [0]
            partner_list = []
        for i in partner_list:
            scopeid_list = []
            if not product_name:
                scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                partner_id=i['partner_id'],version_name=version_name).extra("product_name<>''")
            else:
                scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                partner_id=i['partner_id'],version_name=version_name,product_name=product_name)
            for scope in scopes:
                scopeid_list.append(scope.id)
                scopeid_product[scope.id] = scope.product_name
            result = BasicStat.mgr().get_one_day_model_data_proportion(i['proportion'],scopeid_list,'day',start)
            model_list += result
        count = len(model_list)
        for model in model_list:
            model['product_name'] = scopeid_product[model['scope_id']]
            model['time'] = start.strftime('%Y-%m-%d') 
        
        #find key_list
        if not model_list:
            tmp = []
        else:
            tmp = model_list[0].keys()
        key_list = [i for i in tmp if i not in ('scope_id','time','product_name')]
                
        #find product_name_list
        tmp = []
        for model in model_list:
            tmp.append(model['product_name'])
        tmp_set = set(tmp)
        product_name_list = [i for i in tmp_set]
            
        #get dft res_list
        res_list = []
        for i in product_name_list:
            res_model = {}
            res_model['product_name'] = i
            res_model['time'] = start.strftime('%Y-%m-%d')
            for key in key_list:
                res_model[key] = 0
            res_list.append(res_model)
        #Cumulative data 
        for model in model_list:
            for res in res_list:
                if res['product_name'] == model['product_name']:
                    for key in key_list:
                        res[key] += model[key]
                
        count = len(res_list)
        res_list = self.bubblesort(res_list,order_field)
        #Special factory Coefficient 
        if factory_id in special_factory_list:
            statttime = SPECIAL_FACTORY_COEFFICIENT[factory_id]['starttime']
            sfc = SPECIAL_FACTORY_COEFFICIENT[factory_id]['coefficient']
            if start.strftime('%Y-%m-%d')>= statttime:
                for res in res_list:
                    for i in res.keys():
                        if i not in ('product_name','time'):
                            res[i] = float(res[i]) * coefficient * sfc # coefficient
                            res[i] = long(res[i])
            else:
                for res in res_list:
                    for i in res.keys():
                        if i not in ('product_name','time'):
                            res[i] = float(res[i]) * coefficient # coefficient
                            res[i] = long(res[i])
        #Special factory Coefficient2
        if factory_id in special_factory_list2:
            statttime2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['starttime']
            sfc2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['coefficient']
            if start.strftime('%Y-%m-%d')>= statttime2:
                for res in res_list:
                    for i in res.keys():
                        if i not in ('product_name','time'):
                            res[i] = float(res[i]) * sfc2 # coefficient
                            res[i] = long(res[i])
 
        if action == 'export': 
            title = [('product_name','机型'),('time','时间'),('user_run','启动用户'),('new_user_run','新增启动用户'),('user_visit','访问用户'),
                ('new_user_visit','新增访问用户'),('pay_user','付费用户'),('active_user_visit','活跃用户'),('visits','访问PV'),('cpay_down','按章付费数'),
                ('bpay_down','按本付费数'),('cpay_user','按章付费用户'),('bpay_user','按本付费用户'),('cfree_down','按章免费数'),('bfree_down','按本免费数'),
                ('cfree_user','按章免费用户'),('bfree_user','按本免费用户')]
            xls = Excel().generate(title,res_list,1)
            filename = 'factstat_product_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            res_list = res_list[(page-1)*psize:page*psize]
            self.render('data/accounting_productdaily.html',
                    count=count,psize=psize,page=page,factory_id=factory_id,
                    query_mode=query_mode,product_name=product_name,basics=res_list,
                    start=start.strftime('%Y-%m-%d'),
                    factory_list=factory_list, order_field=order_field)

    def businessdaily(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        factory_id = int(self.get_argument('factory_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        query_mode = self.get_argument('query_mode','accounting_business')
        group = self.get_argument('group','')
        order_field = self.get_argument('orderField','user_run')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = tody - datetime.timedelta(days=30)
        order_field = self.get_argument('orderField','')

        # Special factory
        special_factory_list = SPECIAL_FACTORY_COEFFICIENT.keys()
        special_factory_list2 = SPECIAL_FACTORY_COEFFICIENT2.keys()

        # factory
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],query_mode)
        #get facotry start 
        for fact in factory_list:
            q_start = AccountingFactoryStart.mgr().Q().filter(factory_id=fact['id'])
            if q_start:
                fact['start_time'] = q_start[0]['time']
                user = self.get_current_user()
                if user['name'] in SAFE_USER:
                    fact['start_time'] = datetime.datetime(2012,12,31)
                fact['coefficient'] = q_start[0]['coefficient']
            else:
                fact['start_time'] = None
                user = self.get_current_user()
                if user['name'] in SAFE_USER:
                    fact['start_time'] = datetime.datetime(2012,12,31)
                fact['coefficient'] = 1.0

        basics = []
        count = len(factory_list)
        
        #all_stats = Service.inst().stat.get_all_factstat('day',yest,tody)
        all_stats = Service.inst().stat.get_all_factstat_proportion('day',yest,tody)
        for basic in all_stats:
            all_stats[basic]['sumfee'] = float(all_stats[basic]['consumefee']) + float(all_stats[basic]['msgfee'])
        
        acc_stats = {}
        for i in factory_list:
            factory_id = i['id']
            if not i['start_time']: #start_time
                basic = None
            elif i['start_time'] > yest: 
                basic = None
            else:
                basic = all_stats.get(i.id,None)
            
            if basic:
                if factory_id in special_factory_list:
                    statttime = SPECIAL_FACTORY_COEFFICIENT[factory_id]['starttime']
                    sfc = SPECIAL_FACTORY_COEFFICIENT[factory_id]['coefficient']
                    for k in basic:
                        basic[k] = float(basic[k]) * i['coefficient'] * sfc  #coefficient
                        basic[k] = long(basic[k])
                        acc_stats[k] = basic[k] + acc_stats.get(k,0)
                else:
                    for k in basic:
                        basic[k] = float(basic[k]) * i['coefficient'] #coefficient
                        basic[k] = long(basic[k])
                        acc_stats[k] = basic[k] + acc_stats.get(k,0)
                if factory_id in special_factory_list2:
                    statttime2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['starttime']
                    sfc2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['coefficient']
                    for k in basic:
                        basic[k] = float(basic[k]) * sfc2  #coefficient
                        basic[k] = long(basic[k])
                        acc_stats[k] = basic[k] + acc_stats.get(k,0)

                basic['title'] = i.name
                basics.append(basic)
        acc_stats['title'] = '总计'
        if order_field:
            basics.sort(cmp=lambda x,y:cmp(x[order_field],y[order_field]),reverse=True)
        basics= basics[(page-1)*psize:page*psize]
        if basics:
            basics += [acc_stats]
        groups = Factory.mgr().all_groups()
        self.render('data/accounting_businessdaily.html',
                    count=count,psize=psize,page=page,query_mode=query_mode,basics=basics,
                    factory_id=factory_id,factory_list=factory_list,start=None,product_name='',
                    date=yest.strftime('%Y-%m-%d'),order_field=order_field,group=group,groups=groups)
 
    def businessmonthly(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        factory_id = int(self.get_argument('factory_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        month = self.get_argument('month','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        query_mode = self.get_argument('query_mode','accounting_business')
        group = self.get_argument('group','')
        action = self.get_argument('action','') 
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        last_month = tody - datetime.timedelta(days=30)
        last_month = last_month.strftime('%Y-%m')
        order_field = self.get_argument('orderField','')
        if not month or month == '未选择':
            month = last_month
        start = month + '-01'
        start = datetime.datetime.strptime(start,'%Y-%m-%d')
        next_month_start = start + datetime.timedelta(days=monthrange(start.year,start.month)[1])
        # Special factory
        special_factory_list = SPECIAL_FACTORY_COEFFICIENT.keys()

        # factory
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],query_mode)
        basics = []
        stats = []
        
        for i in factory_list:
            factory_id = i['id']
            partner_list = Partnerv2.mgr().Q().filter(factory_id=i.id)[:]
            if not partner_list:
                continue
            q_start = AccountingFactoryStart.mgr().Q().filter(factory_id=partner_list[0]['factory_id'])
            start_time = None
            user = self.get_current_user()
            if user['name'] in SAFE_USER:
                start_time = datetime.datetime(2012,12,31)
            coefficient = 1.0
            if q_start:
                start_time = q_start[0]['time']
                coefficient = q_start[0]['coefficient']
            
            if not partner_list:
                continue
            if not start_time or start_time >= next_month_start: #start_time
                stat = None
                continue
            elif start_time > start:
                start = start_time
            #stat = MonthlyBusinessStatDaily.mgr().get_monthly_busiess_stat_multy_days(start,next_month_start,partner_list)[0]
            stat = MonthlyBusinessStatDaily.mgr().get_monthly_busiess_stat_multy_days_proportion(start,next_month_start,partner_list)
            # Special factory 
            if factory_id in special_factory_list:
                statttime = SPECIAL_FACTORY_COEFFICIENT[factory_id]['starttime']
                sfc = SPECIAL_FACTORY_COEFFICIENT[factory_id]['coefficient']
                if start.strftime('%Y-%m-%d')>= statttime:
                    stat['fee'] = float(stat['fee']) * sfc 
            # Special factory2
            #if factory_id in special_factory_list2:
            #    statttime2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['starttime']
            #    sfc2 = SPECIAL_FACTORY_COEFFICIENT2[factory_id]['coefficient']
            #    if start.strftime('%Y-%m-%d')>= statttime2:
            #        stat['fee'] = float(stat['fee']) * sfc2 
            
           
            if stat['fee'] != None:
                stat['fee'] = int(stat['fee'])
                stat['factory'] = i.name
            else:
                stat['fee'] = '0'
                stat['factory'] = i.name
            
            # Special factory ...
            if i['id'] == 229 and start.strftime('%Y-%m-%d') == '2014-01-01': 
                stat['fee'] = 483471
            if i['id'] == 2144 and start.strftime('%Y-%m-%d') == '2014-01-01':
                stat['fee'] = 966997

            if i['id'] == 229 and start.strftime('%Y-%m-%d') == '2014-02-01': 
                stat['fee'] = 778841
            if i['id'] == 2144 and start.strftime('%Y-%m-%d') == '2014-02-01':
                stat['fee'] = 1090431

            if i['id'] == 229 and start.strftime('%Y-%m-%d') == '2014-03-01': 
                stat['fee'] = 1027234
            if i['id'] == 2144 and start.strftime('%Y-%m-%d') == '2014-03-01':
                stat['fee'] = 1438187

            if i['id'] == 229 and start.strftime('%Y-%m-%d') == '2014-04-01': 
                stat['fee'] = 1003872
            if i['id'] == 2144 and start.strftime('%Y-%m-%d') == '2014-04-01':
                stat['fee'] = 1405475



            stats.append(stat)
        if order_field:
            stats.sort(cmp=lambda x,y:cmp(x[order_field],y[order_field]),reverse=True)
        
        count = len(stats)
        groups = Factory.mgr().all_groups()
        if action == 'export':
            excel_title = [('factory','厂商名称'),('fee','月收入')]
            xls = Excel().generate(excel_title,stats,1)
            filename = 'accounting_bueiness_monthly_%s.xls' % (start.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/accounting_businessmonthly.html',
                count=count,psize=psize,page=page,query_mode=query_mode,stats=stats,monthly=MONTHLY,
                group=group,groups=groups,month=month,order_field=order_field)

    def bubblesort(self, dict_in_list, order_field):
        for j in range(len(dict_in_list)-1,-1,-1):
            for i in range(j):
                if dict_in_list[i][order_field] < dict_in_list[i+1][order_field]:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list

    def book_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        charge_type = self.get_argument('charge_type','')
        factory_id = int(self.get_argument('factory_id',0))
        order_field = self.get_argument('orderField','fee')
        query_mode = self.get_argument('query_mode','accounting_book')
        group = self.get_argument('group','')
        assert order_field in ('pay_user','free_user','pay_down','free_down','pv','uv','fee','batch_fee','batch_pv','batch_uv','real_fee')
        action = self.get_argument('action','')
        # perm
        q = Factory.mgr().Q()
        group and q.filter(group=group)
        factory_list = self.filter_factory_acc(q[:],query_mode)
        # for safety reason, some time a perm doesn't have a val, and in this Situation a factory CANN'T see all factory accouting data!!!
        cuser = self.get_current_user() 
        if len(factory_list)>150 and (cuser['name'] not in SAFE_USER):
            factory_list = []
        if factory_id == 0 and factory_list:
            factory_id = factory_list[0].id
        partner_list = Partnerv2.mgr().Q().filter(factory_id=factory_id)[:]
        # scope list
        scopeid_list = []
        for i in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=6,run_id=0,partner_id=i.partner_id)[0]
            if scope:
                scopeid_list.append(scope.id)
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')   
        else:
            start = yest
        count,books = 0,[]
        q = BookStat.mgr().get_accouting_multy_scope_stat(scopeid_list,order_field,start)
        charge_type and q.filter(charge_type=charge_type)
        q = q.orderby(order_field,'DESC')
        count = len(q)
        if action == 'export':
            books = q.data()
            for i in books:
                i['batch_fee'] = "%0.2f"%i['batch_fee'] 
        else:
            books = q[(page-1)*psize:page*psize]
            for i in books:
                i['batch_fee'] = "%0.2f"%i['batch_fee'] 
        books = Service.inst().fill_book_info(books)
        # pagination
        page_count = (count+psize-1)/psize
        for book in books:
            book['fee'] = "%.01f"%float(book['fee'])
            book['real_fee'] = "%.01f"%float(book['real_fee'])
        if action == 'export':
            books = Service.inst().fill_book_count_info(books)
            while (self.do_books_have_two_or_empty_title(books)): 
                books = self.remove_books_two_or_empty_title(books) 
            title = [('time','时间'),('book_id','书ID'),('name','书名'),('author','作者'),
                     ('cp','版权'),('category_2','类别'),('category_1','子类'),('category_0','三级分类'),('state','状态'),
                     ('charge_type','计费类型'),('fee','收益'),('real_fee','主账户收益'),('pay_down','付费下载数'),
                     ('pay_user','付费下载用户数'),('free_down','免费下载数'),('free_user','免费下载用户数'),('pv','简介访问数'),('uv','简介访问人数'),
                     ('batch_fee','批量订购阅饼消费')]
            xls = Excel().generate(title,books,1)
            filename = 'book_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/accounting_book.html',
                        page=page, psize=psize,
                        count=count,page_count=page_count,books=books,charge_type=charge_type,
                        order_field = order_field,start=start.strftime('%Y-%m-%d'),factory_id=factory_id,factory_list=factory_list,query_mode=query_mode
                        )




