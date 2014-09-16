#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import urllib2
import logging
import time
from conf.settings import PLAN_API,BOOK_API,SUBJECT_API,KEY_PRE,BOOK_wordCount_chapterCount_API
from conf.settings import API_TRY_TIME_LIMIT,ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML
from tools.send_email import SendEmail

class Extern(object):
    pre = lambda k:KEY_PRE+'_ext_'+k
    lplan = pre('lplan')
    lpartner = pre('lpartner')
    ibook = pre('ibk')
    isubject = pre('isubject')
    _try_times = 0
    def __init__(self, service):
        self.cache = service.cache
        self.sendemail = SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML)

    def get_try_times(self):
        return self._try_times

    def zero_try_times(self):
        self._try_times = 0
        return True

    def acc_try_times(self):
        self._try_times += 1
        return True

    def get_send_email(self):
        return self.sendemail

    def get_plan_list(self, platform_id=6):
        '''
        PLAN_API?platform_id={platform_id}
        '''
        key = '%s_%s' % (self.lplan,platform_id)
        res = self.cache.get(key)
        if not res:
            url = '%s?platform_id=%s'%(PLAN_API,platform_id)
            jsonstr = urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            if data['status'] == 0:
                res = data['res']
                self.cache.set(key,res,3600)
            else:
                self.cache.set(key,res,5)
        return res

    def get_partner_list(self, plan_id):
        '''
        PLAN_API?plan_id={plan_id}
        '''
        key = '%s_%s' % (self.lpartner,plan_id)
        res = self.cache.get(key)
        if not res:
            url = '%s?plan_id=%s'%(PLAN_API,plan_id)
            jsonstr = urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            if data['status'] == 0:
                res = data['res'] if 'res' in data else data['partner_id']
                self.cache.set(key,res,3600)
            else:
                self.cache.set(key,res,5)
        return res

    def get_book_info(self, book_id):
        '''
        BOOK_API?book_id={book_id}
        '''
        key = '%s_%s' % (self.ibook,book_id)
        res = self.cache.get(key)
        if not res:
            url = '%s?book_id=%s'%(BOOK_API,book_id)
            jsonstr = urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            if data['status'] == 0:
                res = data['res']
                self.cache.set(key,res,172800)
            else:
                self.cache.set(key,res,5)
        return res

    def get_book_info2(self, book_id):
        '''
        http://192.168.0.220:8070/cps/getBookDetail?fid=41&p5=19&bookId=10030133
        '''
        key = 'get_book_info2_%s_%s' % (self.ibook,book_id)
        res = self.cache.get(key)
        if not res:
            url = '%s%s'%(BOOK_wordCount_chapterCount_API,book_id)
            try:
                jsonstr = urllib2.urlopen(url,timeout=1).read()
                data = json.loads(jsonstr)
                name = data['bookName']
                author = data['bookAuthor']
                cp = data['fromSource']
                serial = data['completeState']
                parentcategoryname = data['categoryNameV6']
                childcategoryname = data['categoryNameSecond']
                childchildcategoryname = data['categoryNameA']
                tag = data['tag']
                res = {'name':name,'author':author,'cp':cp,'serial':serial,'parentcategoryname':parentcategoryname,
                    'childcategoryname':childcategoryname,'childchildcategoryname':childchildcategoryname,'tag':tag}
            except Exception, e:
                print book_id
                logging.error('%s\n',str(e),exc_info=True)    
            self.cache.set(key,res,172800)
        return res

    def get_book_info_chapterCount_wordCount(self, book_id):
        '''
        BOOK_API2bookid
        http://192.168.0.220:8070/cps/getBookDetail?fid=41&p5=19&bookId=10074579
        '''
        key = '%s_%s_%s' % (self.ibook,book_id,'chapterCount_wordCount')
        stat = self.cache.get(key)
        if not stat:
            chapterCount, wordCount = 0, 0
            url = '%s%s'%(BOOK_wordCount_chapterCount_API,book_id)
            try:
                jsonstr = urllib2.urlopen(url).read()
                data = json.loads(jsonstr)
                chapterCount = data['chapterCount']
                wordCount = data['wordCount']
            except Exception, e:
                print data
                logging.error('%s\n',str(e),exc_info=True)
            stat = {'chapterCount':chapterCount,'wordCount':wordCount}
            self.cache.set(key,stat,172800)
        else:
            chapterCount = stat['chapterCount']
            wordCount = stat['wordCount']
        return chapterCount,wordCount

    def get_subject_info(self, subject_id):
        '''
        SUBJECT_API?id={subject_id}
        '''
        if not subject_id.isdigit():
            return None
        key = '%s_%s' % (self.isubject,subject_id)
        res = self.cache.get(key)
        if not res:
            url = '%s?id=%s'%(SUBJECT_API,subject_id)
            jsonstr = urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            if data['status'] == 0:
                res = data['res']
                self.cache.set(key,res,36000)
            else:
                self.cache.set(key,res,5)
        return res
    
    def get_section_info(self, section_id, type):
        '''
        http://59.151.122.196/api/get_section_info?id=369&type=2
        '''
        data = None

        if type not in (1,2):
            return None
        #url = 'http://59.151.122.196:8080/api/get_section_info?id=%s&type=%s' % (section_id,type)
        #url = 'http://192.168.0.196:8705/api/get_section_info?id=%s&type=%s' % (section_id,type)
        url = 'http://192.168.0.117:8040/api/get_section_info?id=%s&type=%s' % (section_id,type)
        try:
            jsonstr = urllib2.urlopen(url,timeout=0.5).read()
            data = json.loads(jsonstr)
        except Exception, e:
            print url
            logging.error('%s\n',str(e),exc_info=True)
        if not data:
            return None

        if not data['name']:
            data = self.get_subject_info(section_id)
        #print data
        return data

    def get_recharge_log_info(self, phone, start, end):
        '''
        http://192.168.0.228:14004/bill/cs/query_recharging_finish?ucid=&usr=&rgt=&start_date=2013-03-16&stop_date=2013-07-16&card_num=&card_psw=&phone=18845758721&trade_no=
        
        '''
        #url = 'http://192.168.0.228:14004/bill/cs/query_recharging_finish?ucid=&usr=&rgt=&start_date=%s&stop_date=%s&card_num=&card_psw=&phone=%s&trade_no=' % (start,end,phone)
        url = 'http://192.168.0.217:14009/bill/cs/query_recharging_finish?ucid=&usr=&rgt=&start_date=%s&stop_date=%s&card_num=&card_psw=&phone=%s&trade_no=' % (start,end,phone) 
        try:
            jsonstr= urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            res = data
        except Exception, e:
            logging.error('%s\n',str(e),exc_info=True)
            res = None
        return res

    def get_consume_log_info(self, usr, pg, perpg):
        '''
        http://192.168.0.:16000/sail_uc/asset/queryUserBooks?typ=qub&fid=41&p5=19&rgt=7&usr=xingas&pg=1&perpg=20&v3=3
        '''
        url = 'http://192.168.0.157:16000/sail_uc/asset/queryUserBooks?typ=qub&fid=41&p5=19&rgt=7&usr=%s&pg=%s&perpg=%s&v3=3'%(usr,pg,perpg)
        try:
            jsonstr = urllib2.urlopen(url).read()
            data = json.loads(jsonstr)
            res = data
        except Exception, e:
            logging.error('%s\n',str(e),exc_info=True)
            res = None
        return res

    def get_app_name(self,appid):
        '''
        http://192.168.0.196:8084/cps/getAppDetail?appId=18&fid&usr&rgt&channelId
        '''
        #url = 'http://192.168.0.196:8084/cps/getAppDetail?appId=%s&fid&usr&rgt&channelId' % (appid)
        url = 'http://192.168.0.220:8070/cps/getAppDetail?appId=%s&fid&usr&rgt&channelId' % (appid)
        print url
        try:
            if appid == 'unknown':
                res = 'unknown'
            else:
                jsonstr = urllib2.urlopen(url).read()
                data = json.loads(jsonstr)
                res = data['softName']
        except Exception, e:
            logging.error('%s\n',str(e),exc_info=True)
            res = None
        return res

    def get_book_category(self, book_id):
        key = '%s_%s' % ('cate',str(book_id))
        res = self.cache.get(key)
        return res

    def save_book_category(self, book_id , content):
        key = '%s_%s' % ('cate',str(book_id))
        self.cache.set(key,content,172800)
        return True

    def get_category_name(self, category_id):
        key = '%s_%s' % ('categoryid_to_categoryname',str(category_id))
        res = self.cache.get(key)
        return res
    
    def save_category_name(self, category_id, category_name):
        key = '%s_%s' % ('cate',str(category_id))
        self.cache.set(key,category_name,172800)
        return True




