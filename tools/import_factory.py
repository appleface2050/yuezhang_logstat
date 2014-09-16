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

from model.factory import *

def import2db(file_name):
    f = open(file_name)
    for i in f:
        arr = i.split('\t')
        pid,fname = arr[2].strip()[:6],arr[0].strip()
        fact = Factory.mgr(ismaster=1).Q().filter(name=fname)[0]
        if not fact:
            print 'This factory is not in Facotry model...:',fname
            n = Factory.new()
            n.name = fname
            n.group = '其他'
            #print n
            fact = n.save()
        part = Partner.mgr(ismaster=1).Q().filter(partner_id=pid)[0]
        if not part:
            print 'importing new partner_id--->',pid,fname
            p= Partner.new()
            p.partner_id = pid
            p.factory_id = fact.id
            #print p
            p.save()
        else:
            if part.factory_id == fact.id:
                continue
            else:
                #part.factory_id = fact.id
                print 'partner id changed'
            #    print part
        #print pid,fname

if __name__ == '__main__':
    import2db('zhangxingliyi.txt')

