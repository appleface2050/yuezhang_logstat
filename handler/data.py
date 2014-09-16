#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from handler.base import BaseHandler

class DataHandler(BaseHandler):
    def index(self):
        self.render('data/index.html')

