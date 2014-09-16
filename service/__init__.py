#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: service

import json
from lib.mclient import MClient
from conf.settings import MC_SERVERS
from service.extern import Extern
from service.stat import StatCache

class Service(object):
    def __init__(self):
        self.cache = MClient(MC_SERVERS)
        self.extern = Extern(service=self)
        self.stat = StatCache(service=self)
    
    @staticmethod
    def inst():
        name = '_instance'
        if not hasattr(Service, name):
            setattr(Service,name,Service())
        return getattr(Service,name)

    def fill_book_info(self, book_list):
        for i in book_list:
            book = self.extern.get_book_info2(i['book_id'])
            i['name'],i['author'],i['cp'],i['state'] = '','','',''
            i['category_0'],i['category_1'],i['category_2']= '','',''
            if book:
               i['name']  = book['name'].replace(u'《','').replace(u'》','')
               i['author']  = book['author']
               i['cp']  = book['cp']
               i['state']  = '全本' if book['serial'].upper()=='Y' else '连载'
               i['category_0']  = book['parentcategoryname']
               i['category_1']  = book['childcategoryname']
               i['category_2']  = book['childchildcategoryname']
        return book_list

    def fill_book_count_info(self, books):
        for i in books:
            chapterCount, wordCount = self.extern.get_book_info_chapterCount_wordCount(i['book_id'])
            i['chapterCount'] = chapterCount
            i['wordCount'] = wordCount
        return books

    def fill_book_info_by_bid(self, book_list):
        for i in book_list:
            book = self.extern.get_book_info2(i['bid'])
            i['name'],i['author'],i['cp'],i['state'] = '','','',''
            i['category_0'],i['category_1'] = '',''
            if book:
               i['name']  = book['name'].replace(u'《','').replace(u'》','')
               i['author']  = book['author']
               i['cp']  = book['cp']
               i['state']  = '全本' if book['serial'].upper()=='Y' else '连载'
               i['category_0']  = book['parentcategoryname']
               i['category_1']  = book['childcategoryname']
        return book_list

    def fill_subject_info(self, subject_list):
        for i in subject_list:
            subject= self.extern.get_subject_info(i.value)
            if subject:
               i['value']  = subject['name']
        return subject_list

    def fill_section_info(self, subject_list, type):
        for i in subject_list:
            subject= self.extern.get_section_info(i.value,type)
            if subject:
               i['value']  = subject['name']
        return subject_list

    def fill_recharge_log_info(self, phone, start, end):
        return self.extern.get_recharge_log_info(phone,start,end) 
        
    def fill_consume_log_info(self, usr, pg, perpg):
        return self.extern.get_consume_log_info(usr,pg,perpg)

    def fill_app_info(self, stats):
        for app in stats:
            appname = self.extern.get_app_name(app['appid'])
            app['appname'] = appname
        return stats


