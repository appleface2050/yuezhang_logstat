#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import urllib
import datetime
import tornado.web
uibase = tornado.web.UIModule

class BasicQueryCond(uibase):
	def render(self, platform_id, run_id, plan_id, partner_id, version_name, product_name,
               run_list, date=None, unique=None, start=None):
		return self.render_string("ui_mod/basic_query_cond.html",
			platform_id = platform_id,
			run_id = run_id,
			plan_id = plan_id,
			partner_id = partner_id,
			version_name = version_name,
			product_name = product_name,
			run_list = run_list,
			date = date,
			start = start,
			unique = unique,
		)

class IOSBasicQueryCond(uibase):
	def render(self, run_id, partner_id, run_list, date=None, start=None):
		return self.render_string("ui_mod/ios_basic_query_cond.html",
			run_id = run_id,
			partner_id = partner_id,
			run_list = run_list,
			date = date,
			start = start,
		)

class BasicPartnerQueryCond(uibase):
    def render(self, platform_id, run_id, plan_id, partner_id, version_name, product_name,
            run_list, plan_list, date=None, unique=None, start=None):
        return self.render_string("ui_mod/basic_partner_query_cond.html",
            platform_id = platform_id,
            run_id = run_id,
            plan_id = plan_id,
            partner_id = partner_id,
            version_name = version_name,
            product_name = product_name,
            run_list = run_list,
            plan_list = plan_list,
            date = date,
            start = start,
            unique = unique,
        )

class RechargeQueryCond(uibase):
    def render(self, platform_id, run_id, plan_id, partner_id, version_name, product_name,
            run_list, plan_list,
            date=None, unique=None, start=None,):
        return self.render_string("ui_mod/recharge_query_cond.html",
            platform_id = platform_id, 
            run_id = run_id,
            plan_id = plan_id,
            partner_id = partner_id,
            version_name = version_name,
            product_name = product_name,
            run_list = run_list,
            plan_list = plan_list,
            date = date,
            start = start,
            unique = unique,
        )

class FactoryQueryCond(uibase):
	def render(self, factory_list,factory_id, product_name, query_mode=None, start=None, date=None):
		return self.render_string("ui_mod/factory_query_cond.html",
			start = start,
			date = date,
			factory_list = factory_list,
			factory_id = factory_id,
			product_name = product_name,
			query_mode = query_mode,
		)

class FactoryQueryCondWithVersionName(uibase):
	def render(self, factory_list,factory_id, product_name, version_name='', query_mode=None, start=None, date=None):
		return self.render_string("ui_mod/factory_query_cond.html",
			start = start,
			date = date,
			factory_list = factory_list,
			factory_id = factory_id,
			product_name = product_name,
			version_name = version_name,
			query_mode = query_mode,
		)

class EValueQueryCond(uibase):
    def render(self):
        return self.render_string("ui_mod/e_value_query_cond.html")

class MyHomeModule(uibase):
	def render(self):
		return self.render_string("ui_mod/myhome.html")

class Pagination(uibase):
	def render(self, count, page, psize):
		return self.render_string("ui_mod/pagination.html",
			count = count,
			page = page,
			psize = psize,
		)

