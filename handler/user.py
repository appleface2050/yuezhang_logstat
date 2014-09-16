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
from model.user import User,Orgnization,UserRole,Role

class UserHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name','')
        orgid = int(self.get_argument('orgid',0))
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = User.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = User.mgr().Q()
        id and q.filter(id=id)
        name and q.extra("name LIKE '%%%s%%'"%name)
        if orgid:
            org = Orgnization.mgr().one(orgid)
            q.extra("orgid in (%s)"%','.join([str(i.id) for i in org.children()]))
        users = q[(page-1)*psize:page*psize]
        for u in users:
            u.full_org_name = '-'.join([o.name for o in u.org_path()])
        orgs = Orgnization.mgr().Q()
        self.render('user/user_list.html',
                    name = name,
                    orgid = orgid,
                    page = page,
                    psize=psize,
                    count=count,
                    page_count=page_count,
                    users=users,
                    orgs = orgs)
    
    def save(self):
        id = int(self.get_argument('id',0))
        name = self.get_argument('name')
        passwd = self.get_argument('password','')
        is_root = int(self.get_argument('is_root',0))
        is_staff = int(self.get_argument('is_staff',0))
        orgid = int(self.get_argument('orgid',0))
        email = self.get_argument('email','')
        phone = self.get_argument('phone','')
        args = self.request.arguments
        role_ids = [self.get_argument(i,'') for i in self.request.arguments if i.startswith('role__')]
        role_ids = [int(i) for i in role_ids if i]
        print role_ids
        if id:
            u = User.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            u = User.new()
        u.name,u.passwd,u.is_root,u.is_staff,u.orgid = name,passwd,is_root,is_staff,orgid
        u.email,u.phone = email,phone
        u = u.save()
        id = u.id
        cur_roles = [i.role_id for i in UserRole.mgr().Q().filter(uid=id)]
        new_roles = [int(i) for i in role_ids]
        for i in [i for i in cur_roles if i not in new_roles]:
            ur = UserRole.mgr(ismaster=1).Q().filter(uid=id,role_id=i)[0]
            ur and ur.delete()
        for i in [i for i in new_roles if i not in cur_roles]:
            ur = UserRole.new()
            ur.uid,ur.role_id = id,i
            ur.save()
        self.json2dwz('200','closeCurrent','ulist',forwardUrl='user/list')

    def add(self):
        orgs = Orgnization.mgr().Q()
        all_roles = Role.mgr().Q().data()
        self.render('user/user_add.html',
                    orgs = orgs,
                    all_roles = all_roles)

    def edit(self):
        uid = int(self.get_argument('id'))
        user = User.mgr(ismaster=1).Q().filter(id=uid)[0]
        orgs = Orgnization.mgr().Q()
        cur_roles = [i.role_id for i in UserRole.mgr().Q().filter(uid=uid)]
        all_roles = Role.mgr().Q().data()
        self.render('user/user_edit.html',
                    orgs = orgs,
                    cur_roles = cur_roles,
                    all_roles = all_roles,
                    user=user)

    def delete(self):
        uid = int(self.get_argument('id'))
        user = User.mgr(ismaster=1).Q().filter(id=uid)[0]
        user.delete()
        self.json2dwz('200','forward','ulist',forwardUrl='user/list')
    
    def index(self):
        if not self.current_user:
            self.redirect('/user/login')
        else:
            self.render('user/index.html')
    
    def login(self):
        jump = self.get_argument('jump','/') 
        if self.request.method == 'GET':
            if self.current_user:
                self.redirect(jump)
                return
            self.render('user/login.html',jump=jump,error='')
        else:
            name = self.get_argument('name','')
            passwd   = self.get_argument('passwd','')
            u = User.mgr().login(name,passwd)
            print name,passwd,u
            if not u:
                self.render('user/login.html',jump=jump)
            else:
                self._login(u['id'])
                self.redirect(jump)

    def logout(self):
        jump = self.get_argument('jump','/') 
        if self.request.method == 'GET':
            self._logout()
            self.redirect(jump)

    def changepasswd(self):
        self.render('user/user_edit.html')

    def passwd__change(self):
        self.render('user/passwd_change.html')

    def passwd__save(self):
        old_passwd = self.get_argument('old_passwd')
        new_passwd = self.get_argument('new_passwd')
        msg = ''
        if self.current_user:
            u = User.mgr(ismaster=True).one(self.current_user.id)
            if u and u.check_passwd(u.passwd,old_passwd):
                u.passwd = new_passwd
                u.save()
            else:
                msg = 'passwd wrong'
        else:
            msg = 'not logined'
        if not msg:
            self.json2dwz('200','closeCurrent','dlist',forwardUrl='user/list',msg=msg)
        else:
            self.json2dwz('300','closeCurrent','dlist',forwardUrl='user/list',msg=msg)

