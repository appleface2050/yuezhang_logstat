#!/usr/bin/env python
# -*- encoding : utf-8 -*-

import os
import time
import sys
import json
import urllib2
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

import MySQLdb
from conf.settings import API_TRY_TIME_LIMIT,ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML
from tools.send_email import SendEmail
from conf.settings import PLAN_API

class MySQLManager(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host='192.168.0.227',user='analy',passwd='analy123',db='logstatv2',port=3306)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def execute(self, sql):
        if sql == "": return
        self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def commit(self):
        return self.conn.commit()

def ThrdPartnerByPlanRequest(plan_id):
    reslist, flag = [], True 
    url = "%s?plan_id=%d" % (PLAN_API, int(plan_id))
    try:
        #jsonstr = urllib2.urlopen('http://192.168.0.7:10080/scheme?plan_id=%d'%int(plan_id)).read()
        jsonstr = urllib2.urlopen(url).read()
        reslist = json.loads(jsonstr)['partner_id']
    except urllib2.URLError, e:
        print e.reason if hasattr(e, 'reason') else ""
        flag = False 
    finally:
        return reslist, flag

def LoadPlanidList():
    return [34, 35, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]

def SyncThrdPartnerByPlan():
    try_times, wait_time = 3, 60
    manager = MySQLManager()
    plan_list = LoadPlanidList()
    for plan_id in plan_list:
        curr_try_times, partner_list, flag = 0, [], True
        while curr_try_times < try_times:
            partner_list, flag = ThrdPartnerByPlanRequest(plan_id)
            if flag == True: break
            curr_try_times += 1
            time.sleep(wait_time)
        if flag == False and curr_try_times >= try_times:
            SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML).send_mail()
            continue
        if len(partner_list) == 0: continue
        for partner_id in partner_list:
            sql = "select count(1) from sync_thrd_partnerbyplan where plan_id = %d and partner_id = %d" % (int(plan_id), int(partner_id))
            manager.execute(sql)
            entry = manager.fetchone()
            if int(entry[0]) == 0: 
                sql = "insert into sync_thrd_partnerbyplan(plan_id, partner_id) values (%d, %d)" % (int(plan_id), int(partner_id))
                manager.execute(sql)
                manager.commit()

if __name__ == "__main__":
    SyncThrdPartnerByPlan()
    uptime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print uptime
