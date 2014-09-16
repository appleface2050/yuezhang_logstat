#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from model.stat import Scope,BasicStatv3,VisitStat,TopNStat,BookStat,ProductStat
from model.factory import Factory,Partner
from model.stat import FactorySumStat,QueryTimeCount
from service import Service
from lib.excel import Excel
from conf.settings import Query_Limit_Count

class FactStatHandler(BaseHandler):
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
        factory_list = self.filter_factory(q[:],query_mode)
        if factory_id == 0 and factory_list:
            factory_id = factory_list[0].id
        partner_list = Partner.mgr().Q().filter(factory_id=factory_id)[:]
        # scope list
        scopeid_list = []
        for i in partner_list:
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=i.partner_id,version_name=version_name,product_name=product_name)[0]
            if scope:
                scopeid_list.append(scope.id)
        # basic stat
        basics = []
        dft = dict([(i,0) for i in BasicStatv3._fields])
        if query_mode == 'basic':
            start = self.get_argument('start','')
            date = self.get_argument('date','')
            action = self.get_argument('action','')
            if not start:
                start = tody - datetime.timedelta(days=7)
            else:
                start = datetime.datetime.strptime(start,'%Y-%m-%d') 
            _start,days = start,[]
            while _start < tody:
                _end = _start + datetime.timedelta(days=1)
                basic = BasicStatv3.mgr().get_data_by_multi_scope(scopeid_list,mode='day',start=_start,end=_end)
                basic['title'] = _start.strftime('%m-%d')
                basics.append(basic)
                days.append(_start)
                _start = _end
            # for chart
            x_axis = ['%s'%i.strftime('%m-%d') for i in days]
            results = {}
            excludes = ('id','scope_id','mode','time','bfee','cfee','recharge','uptime','batch_fee','batch_pv','batch_uv','user_retention')
            for k in [i for i in BasicStatv3._fields if i not in excludes]:
                results[k] ={'title':BasicStatv3._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in basics])}
            for i in basics:
                i['feesum'] = float(i['consumefee'])+i['msgfee']
            fee_list = [i['feesum'] for i in basics]
            m_fee = '%.1f'%(sum(fee_list)/len(fee_list))
            p_fee = max(fee_list)
            # mean and peak
            basic_m = BasicStatv3.mgr().get_data_by_multi_scope(scopeid_list,'day',start,tody,ismean=True)
            basic_p = BasicStatv3.mgr().get_peak_by_multi_scope(scopeid_list,'day',start,tody)
            basic_m['feesum'],basic_p['feesum'] = m_fee,p_fee
            basic_m['title'],basic_p['title'] = '每日平均','历史峰值'
            basics.extend([basic_m,basic_p])
            if action == 'export':  
                excel_title = [('title','时间'),('user_run','启动用户'),('new_user_run','新增启动用户'),
                ('user_visit','访问用户'),('new_user_visit','新增访问用户'),('pay_user','付费用户'),
                ('active_user_visit','活跃用户'),('visits','访问PV'),
                ('cpay_down','章付费数'),('bpay_down','本付费数'),
                ('cpay_user','章付费用户'),('bpay_user','本付费用户'),
                ('cfree_down','章免费数'),('bfree_down','本免费数'),
                ('cfree_user','章免费用户'),('bfree_user','本免费用户'),
                ('cfee','章月饼消费'),('bfee','本月饼消费')]
                #('cfee','章月饼消费'),('bfee','本月饼消费'),('feesum','消费')]
                #('batch_fee','批量订购阅饼消费'),('batch_pv','批量订购PV'),('batch_uv','批量订购UV')]
                xls = Excel().generate(excel_title,basics,1) 
                filename = 'factstat_%s.xls' % (yest.strftime('%Y-%m-%d'))
                self.set_header('Content-Disposition','attachment;filename=%s'%filename)
                self.finish(xls)
            else:
                self.render('data/factstat.html',
                        platform_id=platform_id,run_id=run_id,plan_id=plan_id,factory_id=factory_id,
                        version_name=version_name,product_name=product_name,query_mode=query_mode,
                        basics=basics,start=start.strftime('%Y-%m-%d'),date=yest.strftime('%Y-%m-%d'),
                        factory_list=factory_list,x_axis=x_axis,results=results
                        )

        elif query_mode == 'product':
            page = int(self.get_argument('pageNum',1))
            psize = int(self.get_argument('numPerPage',20))
            start = self.get_argument('start','')
            date = self.get_argument('date','')
            action = self.get_argument('action','')
            if start != date:
                if not start:
                    start = tody - datetime.timedelta(days=2)
                else:
                    start = datetime.datetime.strptime(start,'%Y-%m-%d')
                if not date:
                    date = yest
                else:
                    date = datetime.datetime.strptime(date,'%Y-%m-%d')
                end = date + datetime.timedelta(days=1)
            
                scopeid_list = []
                scopeid_product = {}
                if factory_id == -1:
                    partnerid_list = [0]
                else:
                    partnerid_list = [i.partner_id for i in partner_list]
                for i in partnerid_list:
                    if product_name != '':
                        scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                                    partner_id=i,version_name=version_name,product_name=product_name).extra("product_name<>''")
                    else:
                        scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                                    partner_id=i,version_name=version_name).extra("product_name<>''")
                        #if len(scopes) > 200 and action != 'export':#limit scope longth, for a better performance, user normally do not review multi-days product detail
                            #scopes = scopes[0:200]
                    for scope in scopes:
                        scopeid_list.append(scope.id)
                        scopeid_product[scope.id] = scope.product_name
                #result = BasicStatv3.mgr().get_multi_days_model_data(scopeid_list,'day',start,end,page,psize,order_field)
                result = BasicStatv3.mgr().get_multi_days_model_data(scopeid_list,'day',start,end)
                count,model_list = result['count'],result['list']
                for model in model_list:
                    model['product_name'] = scopeid_product[model['scope_id']]
                    model['time'] = model['time'].strftime('%Y-%m-%d') 
                model_list = self.cumulative_stat_by_keys(model_list,'time','product_name')
                model_list = self.bubblesort(model_list,'product_name')
                model_list = self.bubblesort_asc(model_list,'time')
                count = len(model_list)
                if action == 'export':
                    title = [('product_name','机型'),('time','时间'),('user_run','启动用户'),('new_user_run','新增启动用户'),('user_visit','访问用户'),
                        ('new_user_visit','新增访问用户'),('pay_user','付费用户'),('active_user_visit','活跃用户'),('visits','访问PV'),('cpay_down','按章付费数'),
                        ('bpay_down','按本付费数'),('cpay_user','按章付费用户'),('bpay_user','按本付费用户'),('cfree_down','按章免费数'),('bfree_down','按本免费数'),
                        ('cfree_user','按章免费用户'),('bfree_user','按本免费用户')]
                    xls = Excel().generate(title,model_list,1)
                    filename = 'factstat_product_%s.xls' % (yest.strftime('%Y-%m-%d'))
                    self.set_header('Content-Disposition','attachment;filename=%s'%filename)
                    self.finish(xls)
                else:
                    model_list = model_list[(page-1)*psize:page*psize]  
                    self.render('data/factstat_product.html',
                        count=count,psize=psize,page=page,factory_id=factory_id,
                        query_mode=query_mode,product_name=product_name,basics=model_list,
                        start=start.strftime('%Y-%m-%d'),date=date.strftime('%Y-%m-%d'),
                        factory_list=factory_list, order_field=order_field
                        )
            else:
                if not start:
                    start = yest
                else:
                    start = datetime.datetime.strptime(start,'%Y-%m-%d')
                if not date:
                    date = yest
                else:
                    date = datetime.datetime.strptime(date,'%Y-%m-%d')
                scopeid_list = []
                scopeid_product = {}
                if factory_id == -1:
                    partnerid_list = [0]
                else:
                    partnerid_list = [i.partner_id for i in partner_list]
                for i in partnerid_list:
                    if not product_name:
                        scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                        partner_id=i,version_name=version_name).extra("product_name<>''")
                    else:
                        scopes = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                        partner_id=i,version_name=version_name,product_name=product_name)
                    for scope in scopes:
                        scopeid_list.append(scope.id)
                        scopeid_product[scope.id] = scope.product_name
                result = BasicStatv3.mgr().get_all_model_data(scopeid_list,'day',yest,tody)
                count,model_list = result['count'],result['list'][:]
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
                    self.render('data/factstat_product_singleday.html',
                        count=count,psize=psize,page=page,factory_id=factory_id,
                        query_mode=query_mode,product_name=product_name,basics=res_list,
                        start=start.strftime('%Y-%m-%d'),date=date.strftime('%Y-%m-%d'),
                        factory_list=factory_list, order_field=order_field
                        )

        elif query_mode == 'one_product':
            page = int(self.get_argument('pageNum',1))
            psize = int(self.get_argument('numPerPage',20))
            start = self.get_argument('start','')
            date = self.get_argument('date','')
            action = self.get_argument('action','')
            product_name = self.get_argument('product_name','')
            if not start:
                start = yest
            else:
                start = datetime.datetime.strptime(start,'%Y-%m-%d')
            #product_name = product_name.upper()
            tody = self.get_date()
            yest = tody - datetime.timedelta(days=1)
            # scope
            if product_name:
                scope = Scope.mgr().Q().filter(product_name=product_name)
            else:
                scope = None
            basics = []
            if scope:
                for i in scope:
                    basic = BasicStatv3.mgr().Q(time=yest).filter(scope_id=i.id,mode='day',time=start)[0]
                    if basic and i['partner_id'] != 0:
                        basic['partner_id'] = i['partner_id']
                        basics.append(basic)
            if basics:
                basics = self.merge_partner(basics)
                for i in basics:
                    factory = Factory.mgr().Q().filter(id=i['factory_id'])[0]
                    i['name'] = factory['name']
                    i['time'] = start.strftime('%Y-%m-%d')
                    i['product_name'] = product_name
            if order_field:
                basics.sort(cmp=lambda x,y:cmp(x[order_field],y[order_field]),reverse=True) 
            count = len(basics)
            basics= basics[(page-1)*psize:page*psize]
            self.render('data/factstat_one_product.html',
                count=count,psize=psize,page=page,query_mode=query_mode,product_name=product_name,basics=basics,
                start=start.strftime('%Y-%m-%d'),order_field=order_field,
            )


        elif query_mode == 'fact' or query_mode == 'business':
            basics = []
            count = len(factory_list)
            all_stats = Service.inst().stat.get_all_factstat('day',yest,tody)
            acc_stats = {}
            for i in factory_list:
                basic = all_stats.get(i.id,None)
                if basic:
                    for k in basic:
                        acc_stats[k] = basic[k] + acc_stats.get(k,0)
                    basic['title'] = i.name
                    basics.append(basic)
             
            acc_stats['title'] = '总计'
            basics.sort(cmp=lambda x,y:cmp(x[order_field],y[order_field]),reverse=True)
            basics= basics[(page-1)*psize:page*psize]
            if basics:
                basics += [acc_stats]
            groups = Factory.mgr().all_groups()
            self.render('data/factstat_fact.html',
                    count=count,psize=psize,page=page,query_mode=query_mode,basics=basics,
                    factory_id=factory_id,factory_list=factory_list,start=None,product_name='',
                    date=yest.strftime('%Y-%m-%d'),order_field=order_field,group=group,groups=groups
                    )
        
        elif query_mode == 'factory_sum':
            start = self.get_argument('start','')
            if not start:
                start = tody - datetime.timedelta(days=7)
            else: 
                start = datetime.datetime.strptime(start,'%Y-%m-%d') 
            #factory sum stat
            fact_sums=[]
            acc_fact_sum={}
            fact_list = [long(i.id) for i in factory_list]
            _start,days = start,[]
            while _start < tody:
                _end = _start + datetime.timedelta(days=1)
                #for the user who access other people's factory sum stat
                if len(fact_list) == 0:
                    _start = _end
                    continue
                
                fact_sum = {}
                in_vals = ','.join(["'%s'"%i for i in fact_list])
                sql = """
                SELECT SUM(visits) AS visits,SUM(imei) AS imei,SUM(user_run) AS user_run,SUM(new_user_run) AS new_user_run,
                SUM(user_visit) AS user_visit, SUM(new_user_visit) AS new_user_visit,SUM(active_user_visit) AS active_user_visit, 
                SUM(user_retention) AS user_retention,SUM(pay_user) AS pay_user, SUM(cpay_down) AS cpay_down, SUM(cfree_down) AS cfree_down,
                SUM(bpay_down) AS bpay_down, SUM(bfree_down) AS bfree_down,SUM(cpay_user) AS cpay_user, SUM(cfree_user) AS cfree_user, 
                SUM(bpay_user) AS bpay_user, SUM(bfree_user) AS bfree_user, SUM(bpay_user) AS bpay_user,SUM(bfree_user) AS bfree_user, SUM(cfee) AS cfee, 
                SUM(bfee) AS bfee FROM factory_sum_stat WHERE time = '%s' AND factory_id IN (%s) """ % (_start,in_vals)
                q = FactorySumStat.mgr().raw(sql)
                if q[0]['visits'] == None:
                    fact_sum = dict([(i,0) for i in FactorySumStat._fields])
                else:
                    fact_sum = q[0]
                for k in fact_sum:
                    acc_fact_sum[k] = fact_sum[k] + acc_fact_sum.get(k,0)
                fact_sum['title'] = _start.strftime('%Y-%m-%d')
                fact_sums.append(fact_sum)
                days.append(_start)
                _start = _end
            acc_fact_sum['title'] = '总计'
            fact_sums.append(acc_fact_sum)
            #for charts
            x_axis = ['%s'%i.strftime('%m-%d') for i in days]
            results = {}
            excludes = ('id','time','factory_id','visits','imei','bfee','cfee','uptime')
            for k in [i for i in FactorySumStat._fields if i not in excludes]: 
                results[k] ={'title':FactorySumStat._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in fact_sums[:-1]])}
            
            #render
            count = len(days)*len(factory_list)
            groups = Factory.mgr().all_groups()
            self.render('data/factstat_sum.html',
                count=count,psize=psize,page=page,query_mode=query_mode,results=results,x_axis=x_axis,fact_sums=fact_sums,
                factory_id=factory_id,factory_list=factory_list,product_name='',start=start.strftime('%Y-%m-%d'),
                date=yest.strftime('%Y-%m-%d'),order_field=order_field,group=group,groups=groups
                )

        elif query_mode == 'recharge_log':
            page = int(self.get_argument('pageNum',1))
            psize = int(self.get_argument('numPerPage',20))
            phone = self.get_argument('phone','')
            
            tody = self.get_date(0)
            start = self.get_argument('start','')
            end = self.get_argument('end','')
            if not start:
                start = tody - datetime.timedelta(days=30)
                start = start.strftime('%Y-%m-%d')
            if not end: 
                end = tody.strftime('%Y-%m-%d')

            #user...
            user = self.get_current_user()
            query_count = QueryTimeCount.mgr().get_user_today_query_count(user['name'],tody.strftime('%Y-%m-%d'))
            if user:
                qt = QueryTimeCount.new()
                qt.user = user['name']
                qt.save()
            
            if query_count<=Query_Limit_Count or user['name']=='admin':
                data = Service.inst().fill_recharge_log_info(phone,start,end)
                recharge_log = data['res']
                if not recharge_log:
                    recharge_log = []
                recharge_log[(page-1)*psize:page*psize]
                is_show = True
            else:
                recharge_log = [] 
                is_show = False
            count = len(recharge_log)
            self.render('data/factstat_recharge_log.html',
                    count=count,psize=psize,page=page,recharge_log=recharge_log,start=start,date=end,
                    query_mode=query_mode,phone=phone,is_show=is_show
                    )

        elif query_mode == 'consume_log':
            page = int(self.get_argument('pageNum',1))
            psize = int(self.get_argument('numPerPage',20))
            usr = self.get_argument('usr','')

            tody = self.get_date(0)
            start = self.get_argument('start','')
            end = self.get_argument('end','')

            #user...
            user = self.get_current_user()
            query_count = QueryTimeCount.mgr().get_user_today_query_count(user['name'],tody.strftime('%Y-%m-%d'))
            if user:
                qt = QueryTimeCount.new()
                qt.user = user['name']
                qt.save()
            dft = {'obj':[],'page':{u'totalPage': 0, u'endPage': 0, u'totals': 0, u'startPage': 1, u'perPages': 20, u'currentPage': 1}}
            obj = [] 
            if query_count<=Query_Limit_Count or user['name']=='admin':
                if not usr:
                    data = dft
                else:
                    data = Service.inst().fill_consume_log_info(usr,page,psize)
                
                obj = data['obj']
                count = data['page']['totals']
                page = data['page']['currentPage']
                psize = data['page']['perPages']
                if not obj:
                    obj = []
                is_show = True
            else:
                obj = []
                count = 0
                page = 1
                psize = psize
                is_show = False
            self.render('data/factstat_consume_log.html',
                count=count,psize=psize,page=page,obj=obj,query_mode=query_mode,is_show=is_show,usr=usr
                )

    def bubblesort(self, dict_in_list, order_field):
        for j in range(len(dict_in_list)-1,-1,-1):   
            for i in range(j):
                if dict_in_list[i][order_field] < dict_in_list[i+1][order_field]:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list


