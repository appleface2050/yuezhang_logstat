#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import urllib
import datetime
import logging
import json
import time
import base64

from conf.settings import SESSION_USER
from lib.utils import time_start
from model.user import User
from model.run import Run
from service import Service
from model.pigstat import A0
from model.stat import PustStat
from model.other import Ebk5Category

class Algorithm(object):
    '''
    some algorithm for handle don't want write in handler's father class and handler file itself, you can write here.
    '''
    def operation_merge_multi_days_books(self, books):
        if not books:
            return None
        elif len(books) == 1:
            return books
        else:
            stats = []
            book_id_list =[]
            
            #find all bookid
            for book in books:
                for i in book:
                    book_id_list.append(i['bookid'])
            book_id_list = list(set(book_id_list))
            
            #init 
            for book_id in book_id_list:
                stat = {}
                stat['bookid'] = book_id
                stat['briefpv'] = 0
                stat['briefuv'] = 0
                stat['downpv'] = 0
                stat['downuv'] = 0
                stats.append(stat)
            
            #acc
            for stat in stats:
                for book in books:
                    for i in book:
                        if i['bookid'] == stat['bookid']:
                            try:
                                stat['briefpv'] += i['briefpv']
                                stat['briefuv'] += i['briefuv']
                                stat['downpv'] += i['downpv']
                                stat['downuv'] += i['downuv']
                            except:
                                continue
            return stats


