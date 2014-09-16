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

from model.user import Resource

class ResourceHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = Resource.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = Resource.mgr().Q()
        id and q.filter(id=id)
        name and q.extra("name LIKE '%%%s%%'"%name)
        resources = q[(page-1)*psize:page*psize]
        self.render('user/resource_list.html',
                    name = name,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    resources = resources)

    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        nick = self.get_argument('nick')
        group = self.get_argument('group')
        attr = self.get_argument('attr','')
        if id:
            r = Resource.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            r = Resource.new()
        r.name,r.nick,r.group,r.attr = name,nick,group,attr
        r = r.save()
        self.json2dwz('200','closeCurrent','ulist',forwardUrl='resource/list')

    def add(self):
        self.render('user/resource_add.html')

    def edit(self):
        rid = int(self.get_argument('id'))
        resource = Resource.mgr(ismaster=1).Q().filter(id=rid)[0]
        self.render('user/resource_edit.html',
                    resource=resource)

    def delete(self):
        rid = int(self.get_argument('id'))
        resource = Resource.mgr(ismaster=1).Q().filter(id=rid)[0]
        resource.delete()
        self.json2dwz('200','forward','ulist',forwardUrl='resource/list')
 
