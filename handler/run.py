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

from model.run import Run

class RunHandler(BaseHandler):
    def list(self):
        id = int(self.get_argument('id',0))
        run_id = int(self.get_argument('run_id',0))
        run_name = self.get_argument('run_name','')
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = Run.mgr().Q().count()
        page_count = (count+psize-1)/psize
        q = Run.mgr().Q()
        id and q.filter(id=id)
        run_id and q.filter(run_id=run_id)
        run_name and q.extra("run_name LIKE '%%%s%%'"%run_name)
        runs = q[(page-1)*psize:page*psize]
        self.render('data/run_list.html',
                    run_id = run_id,
                    run_name = run_name,
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    runs = runs)

    def save(self):
        id = int(self.get_argument('id',0))
        run_id = self.get_argument('run_id')
        run_name = self.get_argument('run_name')
        status = self.get_argument('status')
        if id:
            r = Run.mgr(ismaster=True).Q().filter(id=id)[0]
        else:
            r = Run.new()
        r.run_id,r.run_name,r.status = run_id,run_name,status
        r = r.save()
        self.json2dwz('200','closeCurrent','dlist',forwardUrl='run/list?id=%s'%r.id)

    def add(self):
        self.render('data/run_add.html')

    def edit(self):
        id = int(self.get_argument('id'))
        run = Run.mgr(ismaster=1).one(id)
        self.render('data/run_edit.html',
                    run = run)

    def delete(self):
        id = int(self.get_argument('id'))
        run = Run.mgr(ismaster=1).one(id)
        run.delete()
        self.json2dwz('200','forward','dlist',forwardUrl='run/list')
 
