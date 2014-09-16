#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import copy

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from conf.settings import DIR_CWY_POPIN_RESPONSE,DIR_CWY_POPIN_DOWNLOAD_AMOUNT,DIR_CWY_NONWIFI_POPIN_DOWNLOAD
from model.stat import PopWindowStat 

def popin_datamining_load_data():
    '''
    弹窗数据
    '''
    # part 1
    data = open(DIR_CWY_POPIN_DOWNLOAD_AMOUNT)
    lines = data.readlines()
    dft_popin_stat = {'time':'',
                  'book_id':0,
                  'type':'',
                  'request_user':0,
                  'cd_user':0,
                  'pay_user':0,
                  'real_pay_user':0,
                  'fee':0,
                  'accounting_fee':0,
                  }
    res = []
    for i in lines:
        popin_stat = {}
        try:
            popin = i.strip().split('\t')
            popin_stat = copy.deepcopy(dft_popin_stat)
            popin_stat['time'] = popin[0]
            popin_stat['book_id'] = int(popin[1])
            popin_stat['type'] = popin[2]
            popin_stat['pay_user'] = int(popin[3])
            popin_stat['real_pay_user'] = int(popin[4])
            popin_stat['fee'] = float(popin[5])
            popin_stat['accounting_fee'] = float(popin[6])
            res.append(popin_stat)
        except Exception,e:
            print i
    
    # part 2
    data = open(DIR_CWY_POPIN_RESPONSE)
    lines = data.readlines()
    for i in lines:
        data = i.strip().split('\t')
        for j in res:
            if j['time']==data[0] and j['type']==data[1]:
                j['request_user'] = data[2]
    
    # part 3
    data = open(DIR_CWY_NONWIFI_POPIN_DOWNLOAD)
    lines = data.readlines()
    for i in lines:
        data = i.strip().split('\t')
        for j in res:
            try:
                if j['type'] == 'wifi':
                    continue
                elif j['type']=='非wifi' and j['time']==data[0] and j['book_id']==int(data[1]):
                    j['cd_user'] = int(data[2])
            except Exception,e:
                print i
                print j

    for i in res:
        if i['type'] == '非wifi':
            i['type'] = 'nowifi'
    return res

def stat2db(data):
    for i in data:
        try:
            import_num = 0
            stat = PopWindowStat.new()
            stat.time = i['time']
            stat.book_id = i['book_id']
            stat.type = i['type']
            stat.request_user = i['request_user']
            stat.cd_user = i['cd_user']
            stat.pay_user = i['pay_user']
            stat.real_pay_user = i['real_pay_user']
            stat.fee = i['fee']
            stat.accounting_fee = i['accounting_fee']
            stat.save()
        except Exception,e:
            print i
    return import_num

if __name__ == "__main__":
    res = popin_datamining_load_data()
    stat2db(res) 















