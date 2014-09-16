#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from handler.base import BaseHandler
from model.e_value import EValue,EValueCharegeByBook,EValueStartPointAll,EValueMaxCidMaxFee,EValueFreeDistribute,EValuePayDistribute
from service import Service
from lib.excel import Excel

class EValueHandler(BaseHandler):
    def index(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','usnum')
        charge_type = self.get_argument('charge_type','')
        assert order_field in ('cpay_user','cpay_user_percentage','cfee','ARPU','usnum')
        action = self.get_argument('action','') 
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        startpoint = self.get_argument('startpoint','书架')

        count,books = 0,[]
        q = EValue.mgr().get_all_data(bookid_list,startpoint)
        count = len(q)
        if action == 'export':
            res = q.orderby(order_field,'DESC')
        else:
            res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info_by_bid(res)
        # pagination
        if action == 'export':
            title = [('bid','书ID'),('name','书名'),('cfee','章月饼消费'),
                     ('usnum','总人数'),('cpay_user','付费人数'),('cpay_user_percentage','付费人数/总人数'),
                     ('ARPU','ARPU')]
            xls = Excel().generate(title,books,1)
            filename = 'evalue_chapter.xls'
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/e_value.html',
                plan_id=plan_id,partner_id=partner_id,book_ids=book_ids,startpoint=startpoint,
                page=page,psize=psize,charge_type=charge_type,books=books,
                count=count,order_field=order_field)

    def charge_by_book(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','uv')
        assert order_field in ('pay_user','free_user','fee','uv','e_val','e_val_1wk','e_val_4wk')
        action = self.get_argument('action','') 
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        count,books = 0,[]
        q = EValueCharegeByBook.mgr().get_all_data(bookid_list) 
        count = len(q)
        if action == 'export':
            res = q.orderby(order_field,'DESC')
        else:
            res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info(res)
        
        e_val_fields = ['e_val','e_val_1wk','e_val_4wk']
        for book in books: 
            book['fee'] = '%.0f' % float(book['fee'])
            for i in e_val_fields:
                if book[i] == None:
                    book[i] = 0
        if action == 'export':
            title = [('book_id','书ID'),('name','书名'),('fee','本月饼消费'),
                ('free_user','免费用户数'),('pay_user','付费用户数'),('uv','UV'),
                ('e_val','E值'),('e_val_1wk','一周E值'),('e_val_4wk','四周E值')]
            xls = Excel().generate(title,books,1)    
            filename = 'evalue_book.xls'
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/e_value_charge_by_book.html',
            plan_id=plan_id,partner_id=partner_id,book_ids=book_ids,
            page=page,psize=psize,books=books,
            count=count,order_field=order_field)

    def startpoint(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','shujia')
        assert order_field in ('shucheng','shujia','dabao')
        action = self.get_argument('action','') 
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        count,books = 0,[]
        q = EValueStartPointAll.mgr().get_all_data(bookid_list) 
        count = len(q)
        if action == 'export':
            res = q.orderby(order_field,'DESC')
        else:
            res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info_by_bid(res)
        if action == 'export':
            title = [('bid','书ID'),('name','书名'),('shucheng','书城'),
                ('shujia','书架'),('dabao','打包'),('top1','top1'),('top2','top2'),('top3','top3'),('top4','top4'),('top5','top5'),
                ('top6','top6'),('top7','top7'),('top8','top8'),('top9','top9'),('top10','top10')]
            xls = Excel().generate(title,books,1)    
            filename = 'evalue_startpoint.xls'
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else: 
            self.render('data/e_value_startpoint.html',
            plan_id=plan_id,partner_id=partner_id,book_ids=book_ids,
            page=page,psize=psize,books=books,count=count,order_field=order_field) 

    def max_cid_max_fee(self):
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','maxfee')
        assert order_field in ('maxcid','maxfee')
        action = self.get_argument('action','') 
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        count,books = 0,[]
        q = EValueMaxCidMaxFee.mgr().get_all_data(bookid_list) 
        count = len(q)
        if action == 'export':
            res = q.orderby(order_field,'DESC')
        else:
            res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info_by_bid(res)
        if action == 'export':
            title = [('bid','书ID'),('name','书名'),('author','作者'),('cp','版权'),('category_0','类别'),('category_1','子类'),
                ('charge_type','计费类型'),('state','状态'),('maxcid','最大章节'),('maxfee','最大收入')]
            xls = Excel().generate(title,books,1)    
            filename = 'evalue_max_cid_max_fee.xls'
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            self.render('data/e_value_max_cid_max_fee.html',
            plan_id=plan_id,partner_id=partner_id,book_ids=book_ids,order_field=order_field,
            page=page,psize=psize,books=books,count=count)
    
    def distribute_free(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','d_1')
        assert order_field in ('d_1','d_10','d_20','d_30','d_minue_100')
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        count,books = 0,[]
        q = EValueFreeDistribute.mgr().get_all_data(bookid_list)
        count = len(q)
        res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info_by_bid(res)
        self.render('data/e_value_distribute_free.html',
            order_field=order_field,book_ids=book_ids,page=page,psize=psize,books=books,count=count)

    def distribute_pay(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        order_field = self.get_argument('orderField','')
        assert order_field in ('','p10_usnum','p20_usnum','p30_usnum','p40_usnum','p50_usnum','p60_usnum','p70_usnum','p80_usnum','p90_usnum','p100_usnum')
        book_ids = self.get_argument('book_ids','')  
        bookid_list = [i for i in book_ids.split(',') if i]
        count,books = 0,[]
        q = EValuePayDistribute.mgr().get_all_data(bookid_list)
        count = len(q)
        if order_field is '':
            res = q[(page-1)*psize:page*psize]
        else:
            res = q.orderby(order_field,'DESC')[(page-1)*psize:page*psize]
        books = Service.inst().fill_book_info_by_bid(res)
        self.render('data/e_value_distribute_pay.html',
            order_field=order_field,book_ids=book_ids,page=page,psize=psize,books=books,count=count)
    
        

