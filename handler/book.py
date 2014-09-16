#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from lib.excel import Excel
from handler.base import BaseHandler
from model.stat import Scope,BasicStat,VisitStat,TopNStat
from model.stat import BookStat,BookTagStat,BookReferStat,ProductStat
from model.e_value import EValue
from service import Service
from model.other import Ebk5Category

class BookHandler(BaseHandler):
    def index(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        charge_type = self.get_argument('charge_type','')
        order_field = self.get_argument('orderField','fee')
        assert order_field in ('pay_user','free_user','pay_down','free_down','pv','uv','fee','batch_fee','batch_pv','batch_uv','real_fee')
        action = self.get_argument('action','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')   
        else:
            start = yest
        if start > yest:
            start = yest
        count,books = 0,[]
        if scope:
            if start != yest:
                q = BookStat.mgr().get_data_by_multi_days(scope_id=scope.id,mode='day',start=start,end=tody,charge_type=charge_type)  
                q = q.orderby(order_field,'DESC')
                count = len(q)
                if action == 'export':
                    books = q.data()
                else:
                    books = q[(page-1)*psize:page*psize]
            else:
                q = BookStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)
                charge_type and q.filter(charge_type=charge_type)
                count = q.count()
                q = q.orderby(order_field,'DESC')
                if action == 'export':
                    books = q.data()
                else:
                    books = q[(page-1)*psize:page*psize]
            books = Service.inst().fill_book_info(books)
        # pagination
        page_count = (count+psize-1)/psize
        if action == 'export':
            books = Service.inst().fill_book_count_info(books)
            while (self.do_books_have_two_or_empty_title(books)): 
                books = self.remove_books_two_or_empty_title(books) 
            title = [('time','时间'),('book_id','书ID'),('name','书名'),('author','作者'),
                     ('cp','版权'),('category_2','类别'),('category_1','子类'),('category_0','三级分类'),('state','状态'),
                     ('charge_type','计费类型'),('fee','收益'),('pay_down','付费下载数'),
                     ('pay_user','付费下载用户数'),('free_down','免费下载数'),
                     ('free_user','免费下载用户数'),('pv','简介访问数'),('uv','简介访问人数'),
                     ('batch_fee','批量订购阅饼消费'),('batch_pv','批量订购PV'),('batch_uv','批量订购UV'),
                     ('chapterCount','总章数'),('wordCount','总字数')]
            xls = Excel().generate(title,books,1)
            filename = 'book_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/book.html',
                        platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                        partner_id=partner_id,version_name=version_name,product_name=product_name,
                        run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                        count=count,page_count=page_count,books=books,charge_type=charge_type,
                        order_field = order_field,start=start.strftime('%Y-%m-%d')
                        )
    def chart(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',400))
        charge_type = self.get_argument('charge_type','')
        book_ids = self.get_argument('book_ids','')
        bookid_list = [i for i in book_ids.split(',') if i]
        action = self.get_argument('action','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        else:
            start = tody - datetime.timedelta(days=7)
        # x_axis
        delta = tody - start
        days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
        x_axis = ['%s'%i.strftime('%m-%d') for i in days]
        count,books = 0,[]
        if scope:
            # books of by-book & by-chapter
            result = BookStat.mgr().get_data(scope.id,'day',start,tody,charge_type,bookid_list,page,psize)
            count = result['count']
            books = Service.inst().fill_book_info(result['list'])
            while (self.do_books_have_two_or_empty_title(books)): #this is for Fu Rong's request, but books can't deepcopy, so I have to do this way, so egg pain !
                books = self.remove_books_two_or_empty_title(books)

        # pagination
        results = {}
        excludes = ('id','scope_id','mode','time','uptime','category_1','category_0','charge_type','author','book_id','arpu','category_0')
        for k in [i for i in BookStat._fields if i not in excludes]:
            results[k] ={'title':BookStat._fieldesc[k],'data':','.join([str(i.get(k,0)) for i in books])}
        page_count = (count+psize-1)/psize
        if action == 'export':
            title = [('time','时间'),('book_id','书ID'),('name','书名'),('author','作者'),
                    ('cp','版权'),('category_2','类别'),('category_1','子类'),('category_0','三级分类'),('state','状态'),
                    ('charge_type','计费类型'),('fee','收益'),('pay_down','付费下载数'),
                    ('pay_user','付费下载用户数'),('free_down','免费下载数'),
                    ('free_user','免费下载用户数'),('pv','简介访问数'),('uv','简介访问人数'),
                    ('batch_fee','批量订购阅饼消费'),('batch_pv','批量订购PV'),('batch_uv','批量订购UV')]
            xls = Excel().generate(title,books,1)
            filename = 'book_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/book_chart.html',x_axis=x_axis,results=results,
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                    count=count, page_count=page_count, books=books, charge_type=charge_type,
                    book_ids=book_ids,start=start.strftime('%Y-%m-%d')
                    )
    def tag(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        order_field = self.get_argument('orderField','pay_user')
        assert order_field in ('pay_user','free_user','pay_down','free_down','pv','uv','fee')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) ==len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        count,tags = 0,[]
        if scope:
            q = BookTagStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)
            count = q.count()
            tags = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        # pagination
        page_count = (count+psize-1)/psize
        self.render('data/book_tag.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                    count=count, page_count=page_count, tags=tags, order_field = order_field
                    )
    def refer(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        pkey = self.get_argument('pkey','1S1')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) ==len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        count,tags = 0,[]
        if scope:
            q = BookReferStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest,p_key=pkey)
            count = q.count()
            books = q.orderby('uv','DESC')[(page-1)*psize:page*psize]
            books = Service.inst().fill_book_info(books)
        # pagination
        page_count = (count+psize-1)/psize
        self.render('data/book_refer.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                    count=count, page_count=page_count, books=books, pkey=pkey
                    )

    def category(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        charge_type = self.get_argument('charge_type','')
        order_field = self.get_argument('orderField','fee')
        cate_type = self.get_argument('cate_type','')
        assert order_field in ('pay_user','free_user','pay_down','free_down','pv','uv','fee','batch_fee','batch_pv','batch_uv')
        action = self.get_argument('action','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')   
        else:
            start = yest
        if start > yest:
            start = yest
        count,books = 0,[]
        res_all = {}
        if scope:
            if start != yest:
                q = BookStat.mgr().get_data_by_multi_days(scope_id=scope.id,mode='day',start=start,end=tody,charge_type=charge_type)  
                q = q.orderby(order_field,'DESC')
                count = len(q)
                if action == 'export':
                    books = q.data()
                else:
                    books = q[:]
            else:
                q = BookStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)
                charge_type and q.filter(charge_type=charge_type)
                count = q.count()
                q = q.orderby(order_field,'DESC')
                if action == 'export':
                    books = q.data()
                else:
                    books = q[:]
            books = Service.inst().fill_book_info(books)
            res0,res1,res2 = self.counting_category_stat(books)
            #merge dict
            if not cate_type:
                res_all = dict(dict(res0,**res1),**res2)
            elif cate_type == 'cate0':
                res_all = res0
            elif cate_type == 'cate1':
                res_all = res1
            else:
                res_all = res2
            count = len(res_all.keys())
            #dict->list
            res_list = []
            for i in res_all:
                res_list.append({i:res_all[i]})
            #sort
            res_list = self.bubblesort_double_dict_in_list(res_list,'fee')
            self.render('data/book_category.html',
                        platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                        partner_id=partner_id,version_name=version_name,product_name=product_name,
                        run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                        count=count,res_list=res_list,charge_type=charge_type,cate_type=cate_type,
                        start=start.strftime('%Y-%m-%d')
                        )
 
    def bookcategory(self):
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        charge_type = self.get_argument('charge_type','')
        order_field = self.get_argument('orderField','fee')
        cate_type = self.get_argument('cate_type','')
        action = self.get_argument('action','')
        #perm
        run_list=self.run_list()
        run_list = self.filter_run_id_perms(run_list=run_list)
        run_id_list = [run['run_id'] for run in run_list]      
        if run_id == 0 and run_id_list: # has perm and doesn't select a run_id
            if len(run_id_list) == len(Run.mgr().Q().extra("status<>'hide'").data()):
                run_id = 0  # user has all run_id perms
            else:
                run_id = run_id_list[0]
        if run_id not in run_id_list and run_id != 0: # don't has perm and selete a run_id
            scope = None
        else:
        # scope 
            scope = Scope.mgr().Q().filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                      partner_id=partner_id,version_name=version_name,product_name=product_name)[0]
        tody = self.get_date(1) + datetime.timedelta(days=1)
        yest = tody - datetime.timedelta(days=1)
        last = yest - datetime.timedelta(days=1)
        start = self.get_argument('start','')
        if start:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')   
        else:
            start = yest
        if start > yest:
            start = yest
        count,books = 0,[]
        res_all = {}
        if scope:
            if start != yest:
                q = BookStat.mgr().get_data_by_multi_days(scope_id=scope.id,mode='day',start=start,end=tody,charge_type=charge_type)  
                q = q.orderby(order_field,'DESC')
                count = len(q)
                if action == 'export':
                    books = q.data()
                else:
                    books = q[:]
            else:
                q = BookStat.mgr().Q(time=yest).filter(scope_id=scope.id,mode='day',time=yest)
                charge_type and q.filter(charge_type=charge_type)
                count = q.count()
                q = q.orderby(order_field,'DESC')
                if action == 'export':
                    books = q.data()
                else:
                    books = q[:]
        books = self.get_fill_category_id(books)
        cates = self.counting_category(books)
        cates = self.fill_category_name(cates)
        cates = self.bubblesort(cates,'fee')        
        if action == 'export': 
            title = [('category_no3','类别'),('category_no2','二级分类'),('category_no1','三级分类'),('fee','阅饼消费'),('batch_fee','批量下载阅饼消费')]
            xls = Excel().generate(title,cates,1)
            filename = 'category.xls'
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/book_category2.html',
                    platform_id=platform_id,run_id=run_id,plan_id=plan_id,date=yest.strftime('%Y-%m-%d'),
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),plan_list=self.plan_list(),page=page, psize=psize,
                    count=count,charge_type=charge_type,cates=cates,
                    start=start.strftime('%Y-%m-%d'))
 



