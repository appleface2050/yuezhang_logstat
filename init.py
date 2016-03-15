#!/usr/bin/env python
# -*- coding: utf-8 -*-

# sys
import os
import sys
import logging

# tornado
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

define("port", default=8423, help="run on this port", type=int)
define("debug", default=True, help="enable debug mode")
define("online", default=False, help="enable online mode")
define("project_path", default=sys.path[0], help="deploy_path" )

tornado.options.parse_command_line()

if options.debug:
	import tornado.autoreload

# my...
#	import pylibmc
from lib import uimodules, uimethods
from conf.settings import COOKIE_NAME,COOKIE_SECRET,MC_SERVERS
from lib.session import TornadoSessionManager
from lib.mclient import MClient

# handler
from handler.base import BaseHandler
from handler.index import IndexHandler,MeHandler
from handler.org import OrgHandler
from handler.user import UserHandler
from handler.role import RoleHandler
from handler.perm import PermHandler
from handler.resource import ResourceHandler
from handler.data import DataHandler
from handler.scope import ScopeHandler
from handler.run import RunHandler
from handler.plan import PlanHandler
from handler.factory import FactoryHandler,PartnerHandler
from handler.basic import BasicHandler
from handler.visit import VisitHandler
from handler.book import BookHandler
from handler.topn import TopNHandler
from handler.factstat import FactStatHandler
from handler.e_value import EValueHandler
from handler.wap import WapHandler
from handler.factshow import FactShowHandler
from handler.arpu import ArpuHandler
from handler.moniter import MoniterHandler
from handler.operation import OperationHandler
from handler.apis import APIsHandler
from handler.accounting import AccountingHandler
from handler.factory_accounting import FactoryAccountingHandler
from handler.ios_basic import IOSBasicHandler

#sendmail
from tools.sendmailapi import SendMailHandler

class Application(tornado.web.Application):
	def __init__(self):
		settings = {
			"ui_modules": uimodules,
			"ui_methods": uimethods,
			"static_path": os.path.join(options.project_path, "static"),
			"template_path": os.path.join(options.project_path, "tpl"),
			"xsrf_cookies": False,
			"site_title": "demo",
			"session_mgr": TornadoSessionManager(COOKIE_NAME,COOKIE_SECRET,MClient(MC_SERVERS)),
			"debug": options.debug,
			"online": options.online,
		}
		handlers = [
			(r"/", IndexHandler),
			(r"/me(/[a-zA-Z/]*)?", MeHandler),
			(r"/user(/[a-zA-Z/]*)?", UserHandler),
			(r"/role(/[a-zA-Z/]*)?", RoleHandler),
			(r"/perm(/[a-zA-Z/]*)?", PermHandler),
			(r"/org(/[a-zA-Z/]*)?", OrgHandler),
			(r"/resource(/[a-zA-Z/]*)?", ResourceHandler),
			(r"/data(/[a-zA-Z/]*)?", DataHandler),
			(r"/scope(/[a-zA-Z/]*)?", ScopeHandler),
			(r"/run(/[a-zA-Z/]*)?", RunHandler),
			(r"/plan(/[a-zA-Z/]*)?", PlanHandler),
			(r"/factory(/[a-zA-Z/]*)?", FactoryHandler),
			(r"/partner(/[a-zA-Z/]*)?", PartnerHandler),
			(r"/basic(/[a-zA-Z/]*)?", BasicHandler),
			(r"/visit(/[a-zA-Z/]*)?", VisitHandler),
			(r"/book(/[a-zA-Z/]*)?", BookHandler),
			(r"/topn(/[a-zA-Z/]*)?", TopNHandler),
			(r"/factstat(/[a-zA-Z/]*)?", FactStatHandler),
			(r"/e_value(/[a-zA-Z_/]*)?", EValueHandler),
			(r"/wap(/[a-zA-Z/]*)?", WapHandler),
            (r"/factshow(/[a-zA-Z/]*)?", FactShowHandler),
            (r"/arpu(/[a-zA-Z/]*)?", ArpuHandler),
            (r"/moniter(/[a-zA-Z0-9/]*)?", MoniterHandler),
            (r"/operation(/[a-zA-Z_/]*)?", OperationHandler),
            (r"/APIs(/[a-zA-Z_/]*)?", APIsHandler),
            (r"/sendmail", SendMailHandler),
            (r"/accounting(/[a-zA-Z_/]*)?", AccountingHandler),
            (r"/factory_accounting(/[a-zA-Z_/]*)?", FactoryAccountingHandler),
            (r"/ios_basic(/[a-zA-Z_/]*)?", IOSBasicHandler),
		
        ]
		tornado.web.Application.__init__(self, handlers, **settings)

	def log_request(self, handler):
		status = handler.get_status()
		if status < 400:
			if handler.request.uri[0:7] == '/static':
				return
			log_method = logging.info
		elif status < 500:
			log_method = logging.warning
		else:
			log_method = logging.error
		request_time = 1000.0 * handler.request.request_time()
		if request_time > 30.0 or options.debug or status >= 400:
			log_method("%d %s %.2fms", status, handler._request_summary(), request_time)

if __name__ == "__main__":
	http_server = tornado.httpserver.HTTPServer(Application(), xheaders=True)
	http_server.listen(options.port)
	tornado.ioloop.IOLoop.instance().start()

