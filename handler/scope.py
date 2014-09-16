#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ujson
import datetime
import logging
from conf.settings import STAT_MASK,STAT_MODE
from lib.utils import time_start
from handler.base import BaseHandler
from analyze import HiveQuery 
from model.stat import Scope
from model.run import Run
from service import Service

class ScopeHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        unique = int(self.get_argument('unique',0))
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        # scope 
        q = Scope.mgr().Q()
        id and q.filter(id=id)
        if unique:
            q.filter(platform_id=platform_id,run_id=run_id,plan_id=plan_id,partner_id=partner_id,
                     version_name=version_name,product_name=product_name)
        else:
            platform_id and q.filter(platform_id=platform_id)
            run_id and q.filter(run_id=run_id)
            plan_id and q.filter(plan_id=plan_id)
            partner_id and q.filter(partner_id=partner_id)
            version_name and q.filter(version_name=version_name)
            product_name and q.filter(product_name=product_name)
        # pagination
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = q.count()
        page_count = (count+psize-1)/psize
        scopes = q.orderby('id')[(page-1)*psize:page*psize]
        self.render('data/scope_list.html',
                    unique=unique,platform_id=platform_id,run_id=run_id,plan_id=plan_id,
                    partner_id=partner_id,version_name=version_name,product_name=product_name,
                    run_list=self.run_list(),run_dict=self.run_dict(),
                    page=page, psize=psize, 
                    count=count, page_count=page_count,scopes = scopes
                    )
    def save(self):
        id = int(self.get_argument('id',0))
        platform_id = int(self.get_argument('platform_id',6))
        run_id = int(self.get_argument('run_id',0))
        plan_id = int(self.get_argument('plan_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','').replace('__','.')
        product_name = self.get_argument('product_name','')
        status = self.get_argument('status','pas')
        print 'aa',self.get_argument('mask','')
        mask = [self.get_argument(i,'') for i in self.request.arguments if i.startswith('mask__')]
        mask = ','.join([i for i in mask if i])
        modes = [self.get_argument(i,'') for i in self.request.arguments if i.startswith('mode__')]
        modes = ','.join([i for i in modes if i])
        print mask,modes
        if id:
            s = Scope.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            s = Scope.new()
        s.platform_id,s.run_id,s.plan_id = platform_id,run_id,plan_id
        s.partner_id,s.version_name,s.product_name = partner_id,version_name,product_name
        s.status,s.mask,s.modes = status,mask,modes
        s = s.save()
        self.json2dwz('200','closeCurrent','dlist',forwardUrl='scope/list?id=%s'%s.id)
    
    def add(self):
        self.render('data/scope_add.html',
                     mask = STAT_MASK,
                     modes = STAT_MODE,
                     run_list = self.run_list()
                     )

    def edit(self):
        sid = int(self.get_argument('id'))
        scope = Scope.mgr(ismaster=1).Q().filter(id=sid)[0]
        scope.mask = scope.mask.split(',')
        scope.modes = scope.modes.split(',')
        self.render('data/scope_edit.html',
                     mask = STAT_MASK,
                     modes = STAT_MODE,
                     scope = scope,
                     run_list = self.run_list())

    def delete(self):
        sid = int(self.get_argument('id'))
        scope = Scope.mgr(ismaster=1).Q().filter(id=sid)[0]
        scope and scope.delete()
        self.json2dwz('200','forward','dlist',forwardUrl='scope/list')

