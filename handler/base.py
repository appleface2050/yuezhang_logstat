#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import urllib
import datetime
import logging
import json
import time
import base64
import copy

import tornado.web
from conf.settings import SESSION_USER
from lib.utils import time_start
from model.user import User
from model.run import Run
from service import Service
from model.pigstat import A0
from model.stat import PustStat
from model.other import Ebk5Category
from model.factory import Partner

class BaseHandler(tornado.web.RequestHandler):
    '''
    base handler to implement basic function
    '''
    def get_argument(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True):
        '''
        overide it to encode all the param in utf-8
        '''
        value = super(BaseHandler,self).get_argument(name,default,strip)
        if isinstance(value,unicode):
            value = value.encode('utf-8')
        return value
    
    @property
    def session(self):
        if not hasattr(self,'_session'):
            self._session = self.application.settings['session_mgr'].load_session(self)
        return self._session

    def send_json(self, res, code, msg='', callback=None):
        r = {'status':{'code':code,'msg':msg},'result':res}
        r = json.dumps(r)
        if callback:
            r = '%s(%s);'%(callback,r)
            Gself.set_header('Content-Type', 'application/json')
        self.write(r)

    def parse_module(self, module):
        mod,sub = "",""
        if module:
            arr = module.split("/")
            if len(arr)>=3:
                mod,sub = arr[1],arr[2]
            elif len(arr)>=2:
                mod = arr[1]
        return '%s__%s'%(mod,sub) if sub else mod

    def get(self, module):
        if not self.current_user and self.request.uri != '/user/login':
            self.redirect('/user/login')
            return
        module = self.parse_module(module)
        method = getattr(self,module or 'index')
        if method and module not in ('get','post'):
            method()
        else:
            raise tornado.web.HTTPError(404)

    def post(self, module):
        if not self.current_user and self.request.uri != '/user/login':
            self.redirect('/user/login')
            return
        module = self.parse_module(module)
        method = getattr(self,module or 'index')
        if method and module not in ('get','post'):
            method()
        else:
            raise tornado.web.HTTPError(404)

    def get_current_user(self):
        uinfo,u = self.session[SESSION_USER],None
        if uinfo:
            u = User.mgr().Q().filter(id=uinfo['uid'])[0]
            if not u:
                self._logout()
        return u
    
    def _login(self, uid):
        self.session[SESSION_USER] = {'uid':uid}
        self.session.save()
        
    def _logout(self):
        self.session[SESSION_USER] = None
        self.session.save()

    def get_date(self, delta=0):
        date = self.get_argument('date','')
        if date:
            tody = datetime.datetime.strptime(date,'%Y-%m-%d') 
        else:
            tody = time_start(datetime.datetime.now()-datetime.timedelta(days=delta),'day')
        return tody

    def json2dwz(self, code, cb_type='closeCurrent', navTabId='', forwardUrl='',msg=''):
        '''
        send status response to dwz
        '''
        res = {
                'statusCode':code,
                'callbackType':cb_type,
                'navTabId':navTabId,
                'forwardUrl':forwardUrl,
                'message':msg
            }
        res = json.dumps(res)
        self.write(res)

    def run_dict(self):
        r = {}
        for i in self.run_list():
            r[i.run_id] = i.run_name
        return r

    def run_list(self):
        '''
        run platform list
        '''
        return Run.mgr().Q().extra("status<>'hide'").data()
    
#    def plan_dict(self):
#        r = {}
#        for i in self.plan_list():
#            r[i['id']] = i['name']
#        return r

#    def plan_list(self):
#        '''
#        plan list
#        '''
#        return  Service.inst().extern.get_plan_list()
 
    def has_perm(self, oper, resource, **attr):
        '''
        check perm of current user
        '''
        return self.current_user.has_perm(oper,resource,**attr)

    def filter_factory(self, factory_list, resource='factstat'):
        '''
        filter factory with perm
        '''
        return [i for i in factory_list if self.has_perm('query','factstat:%s'%resource,group=i['group'],factory_id=i['id'])]
   
    def filter_factory_acc(self, factory_list, resource='accounting'):
        '''
        filter factory accounting with perm
        '''
        return [i for i in factory_list if self.has_perm('query','accounting:%s'%resource,group=i['group'],factory_id=i['id'])]

    def filter_wap_factory(self, factory_list, resource='wap_factstat'):
        '''
        filter wap factory with perm
        '''
        return [i for i in factory_list if self.has_perm('query','wap:%s'%resource,group=i['group'],factname=i['name'])]

    def filter_plan_id_perms(self, plan_list, resource='basic'):
        '''
        filter basic plan_id with perm
        '''
        return [i for i in plan_list if self.has_perm('query','basic:%s'%resource,plan_id=i['id'])]

    def filter_run_id_perms(self, run_list, resource='basic'):
        '''
        filter basic run_id with perm
        '''
        return [i for i in run_list if self.has_perm('query','basic:%s'%resource,run_id=i['run_id'])]

    def counting_category_stat(self, books):
        '''
        group category and sum
        '''
        _books = []
        for book in books:
            _book = {}
            if book['category_0'] and book['category_1'] and book['category_2']:
                _book['category_0'] = book['category_0'] 
                _book['category_1'] = book['category_1'] 
                _book['category_2'] = book['category_2'] 
                _book['fee'] = book['fee']
                _book['batch_fee'] = book['batch_fee']
                _books.append(_book)

        #distinct 3 category name
        category_0_list,category_1_list,category_2_list = [],[],[]
        for i in _books:
            if i['category_0'] not in category_0_list:
                category_0_list.append(i['category_0'])
            if i['category_1'] not in category_1_list:
                category_1_list.append(i['category_1'])
            if i['category_2'] not in category_2_list:
                category_2_list.append(i['category_2'])
            
        # init res
        res0,res1,res2 = {},{},{}
        for cate in category_0_list:
            res0[cate] = {'fee':0.0,'batch_fee':0.0}
        for cate in category_1_list:
            res1[cate] = {'fee':0.0,'batch_fee':0.0}
        for cate in category_2_list:
            res2[cate] = {'fee':0.0,'batch_fee':0.0}

        #group by 
        for _book in _books:
            for cate in category_0_list:
                if _book['category_0'] == cate:
                    res0[cate]['fee'] += float(_book['fee'])
                    res0[cate]['batch_fee'] += float(_book['batch_fee'])
            for cate in category_1_list:
                if _book['category_1'] == cate:
                    res1[cate]['fee'] += float(_book['fee'])
                    res1[cate]['batch_fee'] += float(_book['batch_fee'])
            for cate in category_2_list:
                if _book['category_2'] == cate:
                    res2[cate]['fee'] += float(_book['fee'])
                    res2[cate]['batch_fee'] += float(_book['batch_fee'])

        return res0,res1,res2

    def data_process(self, stats, appids, time):
        '''
        recommendation
        '''
        res = []
        for app in appids:
            data = {'appid':app['appid'],'time':time,'install':0,'download':0}
            for stat in stats:
                if app['appid'] == stat['appid']:
                    if stat['type'] == 'install':
                        data['install'] = stat['cnt']
                    elif stat['type'] == 'download':
                        data['download'] = stat['cnt']
            res.append(data)
        return res

    def bubblesort(self, dict_in_list, order_field):
        '''
        eg:
        [{'fee':1,'a':2},{'fee':2,'a':1}]
        '''
        for j in range(len(dict_in_list)-1,-1,-1):   
            for i in range(j):
                if dict_in_list[i][order_field] < dict_in_list[i+1][order_field]:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list

    def bubblesort_asc(self, dict_in_list, order_field):
        '''
        eg:
        [{'fee':1,'a':2},{'fee':2,'a':1}]
        '''
        for j in range(len(dict_in_list)-1,-1,-1):   
            for i in range(j):
                if dict_in_list[i][order_field] > dict_in_list[i+1][order_field]:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list

    def bubblesort_double_dict_in_list(self, dict_in_list, order_field):
        '''
        eg:
        [{u'\u5973\u9891': {'batch_fee': 1, 'fee': 3}}, {u'\u51fa\u7248': {'batch_fee': 2, 'fee': 5}}]
        '''
        for j in range(len(dict_in_list)-1,-1,-1):   
            for i in range(j):
                if dict_in_list[i].get(dict_in_list[i].keys()[0])[order_field] < dict_in_list[i+1].get(dict_in_list[i+1].keys()[0])[order_field]:
                    dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
        return dict_in_list

    def get_pv_uv(self, stats, time):
        '''
        push_info get pv uv
        '''
        #find a0
        for stat in stats:
            spl = stat['msg_url'].split('a0=')
            if len(spl) >= 2:
                stat['a0'] = spl[1].strip('&') #for some error msg_url which has two a0 
            else:
                stat['a0'] = ''
            if stat['a0'].find('&') != -1: # for some msg_url which a0 not in the end of url
                stat['a0'] = stat['a0'].split('&')[0]

        #get pv uv
        for stat in stats:
            if stat['a0'] != '':
                stat['pv'],stat['uv'] = A0.mgr().get_pv_uv_from_a0(stat['a0'],time)
            else:
                stat['pv'],stat['uv'] = 0,0
        return stats
    
    def get_push_user(self, stats, time):
        '''
        get push user
        '''
        for stat in stats:
            if stat['id'] :
                push_user = PustStat.mgr().get_push_user(time,stat['id'])[0]
                if push_user['user_visit']:
                    stat['push_user'] = push_user['user_visit']
                else: 
                    stat['push_user'] = 0
            else:
                stat['push_user'] = 0
        return stats 

    def cumulative_stat_by_keys(self, stats, key1, key2):
        '''
        cumulative dict in list stat
        '''
        time = key1
        product_name = key2
        
        #find key_list
        if not stats:
            tmp = []
        else:
            tmp = stats[0].keys()
        key_list = [i for i in tmp if i not in ('scope_id','time','product_name')]

        #get distinct key1 and key2
        res_list = []
        for stat in stats:
            res = {'bfree_user': 0, 'bfree_down': 0, 'bpay_down': 0, 'new_user_run': 0, 'time': stat['time'], 'cpay_user': 0, 'visits': 0, 'imei': 0, 'cfree_user': 0, 'bfee': 0.0, 'cfee': 0.0, 'pay_user': 0, 'user_retention': 0, 'new_user_visit': 0, 'user_run': 0, 'cfree_down': 0, 'bpay_user': 0, 'batch_pv': 0, 'user_visit': 0, 'batch_fee': 0.0, 'active_user_visit': 0, 'cpay_down': 0, 'product_name': stat['product_name'], 'batch_uv': 0}
            res_list.append(res)
        
        #distinct
        res_list_dist = []
        for i in res_list:
            if not i in res_list_dist:
                res_list_dist.append(i)
        #res_list_dist = self.get_distinct_key(res_list)
        #acc
        for stat in stats:
            for res in res_list_dist:
                if (res[time] == stat[time] and res[product_name] == stat[product_name]):
                    for key in key_list:
                        res[key] += stat[key]

        return res_list_dist

    def get_distinct_key(self,res_list):
        res_list_dist = []
        dist_time = []
        dist_product_name = []
        for i in res_list:
            dist_time.append(i['time'])
            dist_product_name.append(i['product_name'])
        dist_time = list(set(dist_time))
        dist_product_name = list(set(dist_product_name))
        for product_name in dist_product_name:
            for time in dist_time:
                res = {'bfree_user': 0, 'bfree_down': 0, 'bpay_down': 0, 'new_user_run': 0, 'time': time, 'cpay_user': 0, 'visits': 0, 'imei': 0, 'cfree_user': 0, 'bfee': 0.0, 'cfee': 0.0, 'pay_user': 0, 'user_retention': 0, 'new_user_visit': 0, 'user_run': 0, 'cfree_down': 0, 'bpay_user': 0, 'batch_pv': 0, 'user_visit': 0, 'batch_fee': 0.0, 'active_user_visit': 0, 'cpay_down': 0, 'product_name': product_name, 'batch_uv': 0}
                res_list_dist.append(res)
        return res_list_dist

    def get_fill_category_id(self,books):
        for book in books:
            cate = Service.inst().extern.get_book_category(book['book_id'])
            if not cate:
                #print "not memcached ",book['book_id']
                try:
                    q = Ebk5Category.mgr().get_cate_ID_by_bookid(book['book_id'])
                    if q:
                        q = q[0]
                        book['category_no1'] = q['category1']
                        book['category_no2'] = q['category2']
                        book['category_no3'] = q['category3']
                        Service.inst().extern.save_book_category(book['book_id'],q)
                    else:
                        book['category_no1'] = ''
                        book['category_no2'] = ''
                        book['category_no3'] = ''
                except Exception, e:
                    print book['book_id'],'can not find category'
                    logging.error('%s\n',str(e),exc_info=True)
            else:
                book['category_no1'] = cate['category1']
                book['category_no2'] = cate['category2']
                book['category_no3'] = cate['category3']
        return books 

    def counting_category(self, books):
        #find distinct category
        cate_list = []
        res = []
        for book in books:
            if book['category_no1']!='' and book['category_no1'] not in cate_list:
                cate_list.append(book['category_no1'])
        #init
        for cate in cate_list:
            res.append({'category_no1':cate, 'batch_fee':0.0, 'fee':0.0})
        #acc
        for i in res:
            for book in books:
                if i['category_no1'] == book['category_no1']:
                    i['category_no2'] = book['category_no2']
                    i['category_no3'] = book['category_no3']
                    i['batch_fee'] += book['batch_fee']
                    i['fee'] += book['fee']
        return res

    def fill_category_name(self, cates):
        category_list = ['category_no1','category_no2','category_no3']
        for cate in cates:
            for category in category_list:
                cate_name_from_mc = Service.inst().extern.get_category_name(cate[category])
                if not cate_name_from_mc:
                    try:
                        cate_name = Ebk5Category.mgr().get_cate_name_by_cateID(cate[category])
                        if cate_name:
                            Service.inst().extern.save_category_name(cate[category],cate_name[0]['category_name'])
                            cate[category] = cate_name[0]['category_name']
                        else:
                            cate[category] = ''
                    except Exception, e:
                        logging.error('%s\n',str(e),exc_info=True)                        
                else:
                    cate[category] = cate_name_from_mc
        return cates

    def do_books_have_two_or_empty_title(self, books):
        for book in books:
            if str(book['book_id'])[0] == '2' or book['name'] == '':
                return True
        return False
        
    def remove_books_two_or_empty_title(self, books):
        for book in books:
            if str(book['book_id'])[0] == '2' or book['name'] == '':
                books.remove(book)
        return books

    def merge_partner(self, basics):
        if not basics:
            return None
        for i in basics:
            partner = Partner.mgr().Q().filter(partner_id=i['partner_id'])[0] 
            if not partner: #a partner_id in Scope but not in Partner 
                i['factory_id'] = None
            else:
                i['factory_id'] = partner['factory_id']
        factory_id_list = [i['factory_id'] for i in basics if i['factory_id'] != None]
        factory_id_list = list(set(factory_id_list))
        # merge factory
        result = []
        dft = {'bfree_user': 0, 'bfree_down': 0, 'bpay_down': 0, 'new_user_run': 0, 'visits': 0, 'imei': 0, 'cfree_user': 0, 
            'bfee': 0.0, 'cfee': 0.0, 'pay_user': 0, 'user_retention': 0, 'new_user_visit': 0, 'user_run': 0, 'cpay_user': 0,
            'cfree_down': 0, 'bpay_user': 0, 'batch_pv': 0, 'user_visit': 0, 'batch_fee': 0.0, 'active_user_visit': 0, 'cpay_down': 0, 
            'batch_uv': 0 }
        for i in factory_id_list:
            res = {}
            res = copy.deepcopy(dft)
            res['factory_id'] = i
            result.append(copy.deepcopy(res))
        for res in result:
            for i in basics:
                if res['factory_id'] == i['factory_id']:
                    for item in dft:
                        res[item] += i[item]
        return result       



