#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ujson
import logging
import tornado
from handler.base import BaseHandler
from model.factory import Factory,Partner
from model.stat import Partnerv2

class FactoryHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        group = self.get_argument('group','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        q = Factory.mgr().Q()
        id and q.filter(id=id)
        name and q.extra("name LIKE '%%%s%%'"%name)
        group and q.filter(group=group)
        count = q.count()
        factories = q[(page-1)*psize:page*psize]
        groups = Factory.mgr().all_groups()
        self.render('data/factory_list.html',
                    name = name,
                    group = group,
                    page = page,
                    psize = psize,
                    count = count,
                    factories = factories,
                    groups = groups)
    def search(self):
        name = self.get_argument('factory_name','')
        resource = self.get_argument('resource','basic')
        q = Factory.mgr().Q(qtype='select id,id as factory_id,name as factory_name,`group`')
        name and q.extra("name LIKE '%%%s%%'"%name)
        factories = self.filter_factory(q.data(),resource)[:10]
        self.write(ujson.dumps(factories))

    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        group = self.get_argument('group')
        email = self.get_argument('email','')
        phone = self.get_argument('phone','')
        intro = self.get_argument('intro','')
        status = self.get_argument('status')
        if id:
            f = Factory.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            f = Factory.new()
        f.name,f.group,f.email,f.phone,f.intro,f.status = name,group,email,phone,intro,status
        f = f.save()
        self.json2dwz('200','closeCurrent','dlist',forwardUrl='factory/list?id=%s'%f.id)

    def add(self):
        groups = Factory.mgr().all_groups()
        self.render('data/factory_add.html',
                    groups = groups)

    def edit(self):
        id = int(self.get_argument('id'))
        factory = Factory.mgr(ismaster=1).one(id)
        groups = Factory.mgr().all_groups()
        self.render('data/factory_edit.html',
                    factory = factory,
                    groups = groups)

    def delete(self):
        id = int(self.get_argument('id'))
        factory = Factory.mgr(ismaster=1).one(id)
        factory.delete()
        self.json2dwz('200','forward','dlist',forwardUrl='factory/list')

class PartnerHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        factory_id = int(self.get_argument('factory_id',0))
        partner_id = int(self.get_argument('partner_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        q = Partner.mgr().Q()
        count = q.count()
        page_count = (count+psize-1)/psize
        id and q.filter(id=id)
        factory_id and q.filter(factory_id=factory_id)
        partner_id and q.filter(partner_id=partner_id)
        partners = q[(page-1)*psize:page*psize]
        for i in partners:
            i.factory = Factory.mgr().one(i.factory_id)
        factories = Factory.mgr().Q().data()
        self.render('data/partner_list.html',
                    partner_id = partner_id,
                    factory_id = factory_id,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    partners = partners,
                    factories = factories)

    def save(self):
        id = int(self.get_argument('pid',0))
        #id = int(self.get_argument('id',0))
        #factory_id = int(self.get_argument('factory_id'))
        partner_id = int(self.get_argument('partner_id'))
        factory_name = self.get_argument('factory_name')
        factory = Factory.mgr().Q().filter(name=factory_name)
        factory_id = factory[0]['id']
        if id:
            p = Partner.mgr(ismaster=True).Q().filter(id=id)[0]
            p2 = Partnerv2.mgr(ismaster=True).Q().filter(factory_id=p['factory_id'])[0]
        else:
            p = Partner.new()
            p2 = Partnerv2.new()
        p.factory_id,p.partner_id = factory_id,partner_id
        p2.factory_id,p2.partner_id = factory_id,partner_id
        p = p.save()
        p2.save()
        self.json2dwz('200','closeCurrent','dlist',forwardUrl='partner/list?id=%s'%p.id)

    def add(self):
        factories = Factory.mgr().Q().data()
        self.render('data/partner_add.html',
                    factories = factories)

    def edit(self):
        id = int(self.get_argument('id'))
        partner = Partner.mgr(ismaster=1).one(id)
        factories = Factory.mgr().Q().data()
        self.render('data/partner_edit.html',
                    partner = partner,
                    factories = factories)

    def delete(self):
        id = int(self.get_argument('id'))
        partner = Partner.mgr(ismaster=1).one(id)
        partner2 = Partnerv2.mgr(ismaster=True).Q().filter(partner_id=partner['partner_id'])
        partner.delete()
        for i in partner2:
            i.delete()
        self.json2dwz('200','forward','dlist',forwardUrl='partner/list')
 
