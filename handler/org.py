#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ujson
import logging
import tornado
from handler.base import BaseHandler
from model.user import Orgnization

class OrgHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        parent_id = int(self.get_argument('parent_id',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = Orgnization.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = Orgnization.mgr().Q()
        id and q.filter(id=id)
        name and q.filter(name=name)
        parent_id and q.filter(parent=parent_id)
        orgs = q[(page-1)*psize:page*psize]
        for i in orgs:
            i.parent = Orgnization.mgr().one(i.parent)
        all_orgs = Orgnization.mgr().Q().data()
        self.render('user/org_list.html',
                    name = name,
                    parent_id = parent_id,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    orgs = orgs,
                    all_orgs = all_orgs)

    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        pid = int(self.get_argument('parent_id',0))
        parent = Orgnization.mgr().one(pid) if pid else None
        if not parent:
            pid,level = 0,0
        else:
            level = parent.level + 1 
        if id:
            o = Orgnization.mgr(ismaster=True).one(id)
        else:
            o = Orgnization.new()
        o.name,o.level,o.parent = name,level,pid
        try:
            o = o.save()
            self.json2dwz('200','closeCurrent','ulist',forwardUrl='org/list')
        except Exception,e:
            self.json2dwz('300','closeCurrent','ulist',forwardUrl='org/list',msg=str(e))

    def add(self):
        orgs = Orgnization.mgr().Q().data()
        self.render('user/org_add.html',
                    orgs = orgs)

    def edit(self):
        oid = int(self.get_argument('id'))
        org = Orgnization.mgr(ismaster=1).one(oid)
        orgs = Orgnization.mgr().Q().data()
        self.render('user/org_edit.html',
                    org = org,
                    orgs = orgs)

    def delete(self):
        oid = int(self.get_argument('id'))
        org = Orgnization.mgr(ismaster=1).one(oid)
        org.delete()
        self.json2dwz('200','forward','ulist',forwardUrl='org/list')
 
