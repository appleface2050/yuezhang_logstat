#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import sys
import json
import logging
import urllib2
import getopt
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat,MonthlyBusinessStatDaily,AccountingFactoryStart
from model.factory import Factory,Partner
from service import Service

if __name__ == "__main__":
    start = datetime.datetime(2014,4,1)
    end = datetime.datetime(2014,5,1)
    while start < end:
        print start
        _start = start
        _end = _start + datetime.timedelta(days=1)
        
        all_stats = Service.inst().stat.get_all_partner_fee_daily('day',_start,_end)
        for i in all_stats.keys():
            coefficient = AccountingFactoryStart.mgr().get_coefficient_by_partner_id(i)
            if coefficient:
                s = MonthlyBusinessStatDaily.new()
                try:
                    s.time = start
                    s.partner_id = i
                    s.fee = all_stats[i] * coefficient[0]['coefficient']
                    #s.fee =  "%.01f" % s.fee
                    s.fee = int(s.fee)
                    s.save()
                except Exception,e:
                    logging.error('%s\n',str(e),exc_info=True)
        start += datetime.timedelta(days=1) 






