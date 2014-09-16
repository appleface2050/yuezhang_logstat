#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from lib.utils import time_start
from handler.base import BaseHandler
from conf.settings import PAGE_TYPE
from model.stat import Scope,BasicStat,ArpuOneWeekFee
from model.run import Run
from service import Service
from tools.send_email import SendEmail

class APIsHandler(BaseHandler):

    def info(self):
        print 'xxxx'
        id = self.get_argument("id", "")
        self.write(id)

    def sendmail(self):
        send_to = self.get_argument("to", "")
        subject = self.get_argument("sub", "") 
        context = self.get_argument("context", "")
        if send_to.find(','):
            send_to_list = send_to.split(',')
        else:
            send_to_list = [send_to]
        
        s = SendEmail(send_to_list, subject, context)
        if s.send_mail():
            self.write("Succeed")
        else:
            self.write("Failed")
       




