#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import ujson
import logging
import urllib
import urlparse
import re
import tornado
from handler.base import BaseHandler
from model.user import Perm,Resource,PermAttr

class PermHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = Perm.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = Perm.mgr().Q()
        id and q.filter(id=id)
        name and q.extra("name LIKE '%%%s%%'"%name)
        perms = q[(page-1)*psize:page*psize]
        self.render('user/perm_list.html',
                    name = name,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    perms = perms)

    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        oper = self.get_argument('oper')
        resource_id = int(self.get_argument('resource_id',0))
        resource = Resource.mgr(ismaster=1).Q().filter(id=resource_id)[0]
        all_attrs = resource.attr.split(':') if resource and resource.attr else []
        perm_attr = {}
        for i in [a for a in all_attrs if a]:
            perm_attr[i] = self.get_argument(i,'')
        if id:
            p = Perm.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            p = Perm.new()
        print id,name,oper,resource_id,perm_attr
        p.name,p.oper,p.resource_id = name,oper,resource_id
        p = p.save()
        for i in perm_attr:
            pa = PermAttr.mgr(ismaster=1).Q().filter(perm_id=p.id,attr_name=i)[0]
            if pa:
                pa.attr_val = perm_attr[i]
            else:
                pa = PermAttr.new() 
                pa.perm_id,pa.attr_name,pa.attr_val = p.id,i,perm_attr[i]
            pa.save()
        self.json2dwz('200','closeCurrent','ulist',forwardUrl='perm/list')

    def add(self):
        resources = Resource.mgr(ismaster=1).Q()[:]
        self.render('user/perm_add.html',
                    resources = resources)

    def edit(self):
        pid = int(self.get_argument('id'))
        perm = Perm.mgr(ismaster=1).Q().filter(id=pid)[0]
        resource = Resource.mgr(ismaster=1).Q().filter(id=perm.resource_id)[0]
        all_attrs = resource.attr.split(':') if resource and resource.attr else []
        _perm_attr = dict([(i.attr_name,i.attr_val) for i in PermAttr.mgr().Q().filter(perm_id=pid)[:]])
        perm_attr = {}
        for i in all_attrs:
            perm_attr[i] = _perm_attr.get(i,'') 
        resources = Resource.mgr(ismaster=1).Q()[:]
        print perm_attr
        self.render('user/perm_edit.html',
                    perm=perm,
                    resource = resource,
                    resources = resources,
                    perm_attr = perm_attr)

    def delete(self):
        pid = int(self.get_argument('id'))
        perm = Perm.mgr(ismaster=1).Q().filter(id=pid)[0]
        perm.delete()
        self.json2dwz('200','forward','ulist',forwardUrl='perm/list')
 
