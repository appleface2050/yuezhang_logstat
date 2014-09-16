#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# Abstract: import factory

import os
import sys
import getopt
import datetime
import logging
import fcntl
import errno

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from model.stat import Partnerv2,Partner

def import_to_partnerv2():
    partner = Partner.mgr().Q().data()
    for i in partner:
        s = Partnerv2.new()
        try:
            s.partner_id = i['partner_id']
            s.factory_id = i['factory_id']
            s.proportion = 1.0
            s.save()
        except Exception,e: 
            print i
            logging.error('%s\n',str(e),exc_info=True)

if __name__ == '__main__':
    import_to_partnerv2()




