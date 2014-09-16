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

from model.user import Role,Perm,RolePerm

class RoleHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = Role.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = Role.mgr().Q()
        id and q.filter(id=id)
        name and q.extra("name LIKE '%%%s%%'"%name)
        roles = q[(page-1)*psize:page*psize]
        self.render('user/role_list.html',
                    name = name,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    roles = roles)

    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        perm_ids = [self.get_argument(i,'') for i in self.request.arguments if i.startswith('perm__')]
        perm_ids = [int(i) for i in perm_ids if i]
        print 'perm_ids:',perm_ids
        if id:
            r = Role.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            r = Role.new()
        r.name = name
        r = r.save()
        id = r.id
        cur_perms = [i.perm_id for i in RolePerm.mgr().Q().filter(role_id=id)]
        new_perms = [int(i) for i in perm_ids]
        for i in [i for i in cur_perms if i not in new_perms]:
            rp = RolePerm.mgr(ismaster=1).Q().filter(role_id=id,perm_id=i)[0]
            rp and rp.delete()
        for i in [i for i in new_perms if i not in cur_perms]:
            rp = RolePerm.new()
            rp.role_id,rp.perm_id = id,i
            rp.save()
        self.json2dwz('200','closeCurrent','ulist',forwardUrl='role/list')
    
    def add(self):
        all_perms = Perm.mgr().Q().data()
        self.render('user/role_add.html',
                    all_perms= all_perms)

    def edit(self):
        rid = int(self.get_argument('id'))
        role = Role.mgr(ismaster=1).Q().filter(id=rid)[0]
        cur_perms = [i.perm_id for i in RolePerm.mgr().Q().filter(role_id=rid)]
        all_perms = Perm.mgr().Q().data()
        self.render('user/role_edit.html',
                    role=role,
                    cur_perms=cur_perms,
                    all_perms= all_perms)
    
    def delete(self):
        rid = int(self.get_argument('id'))
        role = Role.mgr(ismaster=1).Q().filter(id=rid)[0]
        role.delete()
        for i in RolePerm.mgr(ismaster=1).Q().filter(role_id=rid):
            i.delete()
        self.json2dwz('200','forward','ulist',forwardUrl='role/list')
 
