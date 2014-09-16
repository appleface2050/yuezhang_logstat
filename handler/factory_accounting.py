#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ujson
import logging
import tornado
import datetime
from handler.base import BaseHandler
from model.factory import Factory,Partner
from model.stat import AccountingFactoryStart

class FactoryAccountingHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        if not name:
            q = AccountingFactoryStart.mgr().Q()
        else:
            fid = Factory.mgr().Q().extra("name LIKE '%%%s%%'"%name)
            if fid:
                fidlist = ','.join([str(i['id']) for i in fid])
                q = AccountingFactoryStart.mgr().Q().extra("factory_id in (%s)"%fidlist)
            else:
                q = []
        count = len(q)
        factories = q[(page-1)*psize:page*psize]
        for i in factories:
            i['time'] = i['time'].strftime('%Y-%m-%d')
        if factories:
            for i in factories:
                try: 
                    factory_name = Factory.mgr().Q().filter(id=i.factory_id)[0]['name']
                except Exception, e:
                    factory_name = ''
                i['factory_name'] = factory_name
        self.render('data/factory_accounting_list.html',name=name,page=page,psize=psize,count=count,factories=factories
                    )

    def search(self):
        name = self.get_argument('factory_name','')
        resource = self.get_argument('resource','accounting_basic')
        q = Factory.mgr().Q(qtype='select id,id as factory_id,name as factory_name,`group`')
        name and q.extra("name LIKE '%%%s%%'"%name)
        factories = self.filter_factory_acc(q.data(),resource)[:10]
        self.write(ujson.dumps(factories))

    def save(self):
        factory_id = self.get_argument('factory_id')
        time = str(self.get_argument('time'))
        time = datetime.datetime.strptime(time,'%Y-%m-%d')
        coefficient = float(self.get_argument('coefficient'))
        author = self.get_current_user()['name']
        if factory_id:
            f = AccountingFactoryStart.mgr(ismaster=True).Q().filter(factory_id=factory_id)[0]
            if not f:
                f = AccountingFactoryStart.new()
        f.time = time
        f.factory_id = factory_id
        f.coefficient = coefficient
        f.author = author
        f = f.save()
        self.json2dwz('200','closeCurrent','dlist',forwardUrl='factory_accounting/list')

    def add(self):
        #groups = Factory.mgr().all_groups()
        self.render('data/factory_accounting_add.html',
                    )

    def edit(self):
        id = int(self.get_argument('id'))
        factory = AccountingFactoryStart.mgr(ismaster=1).Q().filter(factory_id=id)[0]
        self.render('data/factory_accounting_edit.html',
                    factory = factory,
                    )

    def delete(self):
        id = int(self.get_argument('id'))
        #factory = Factory.mgr(ismaster=1).one(id)
        factory = AccountingFactoryStart.mgr(ismaster=1).Q().filter(factory_id=id)[0]
        #print factory
        factory and factory.delete()
        self.json2dwz('200','forward','dlist',forwardUrl='factory_accounting/list')

 
