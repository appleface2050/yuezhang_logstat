#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ujson
import logging
import tornado
from handler.base import BaseHandler

class PlanHandler(BaseHandler):
    def list(self):
        plans = self.plan_list()
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',20))
        count = len(plans)
        page_count = (count+psize-1)/psize
        self.render('data/plan_list.html',
                    page = page,
                    psize = psize,
                    count = count,
                    page_count = page_count,
                    plans = plans)

