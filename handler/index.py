#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import urllib
import string
import datetime
import tornado.web
from handler.base import BaseHandler

class IndexHandler(BaseHandler):
    def get(self):
        if not self.current_user:
            self.redirect('/user/login')
        else:
            self.render("index.html")

class MeHandler(BaseHandler):
    def index(self):
        self.render("me/index.html")

    def home(self):
        self.render("me/home.html")

