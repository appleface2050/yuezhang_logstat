#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
from handler.base import BaseHandler
from lib.excel import Excel
from conf.settings import WAP_PAGE_TYPE,PUSH_VERSION
from model.recommendation import Recommendation
from model.stat import PushStat,BkrackMsg,PustStat,Qiandao,QiandaoBasic,QiandaoRecharge,OperaBaoyueSum,OperaBaoyueBook,TxtRecommendation,OperateCenter
from model.stat import VPCustom,VPCancel
from service import Service
from model.pigstat import A0
from handler.algorithm import Algorithm

class OperationHandler(BaseHandler, Algorithm):
    def push(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]

        stats = PushStat.mgr().get_push_stat(start,end)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/push.html',
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def push_info(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        pid = self.get_argument('pid','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        ds = self.get_argument('ds','')
        if not start:
            start = tody - datetime.timedelta(days=120)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = tody
        count,stats = 0,[]
        MAX_PARTNER_SHOW = 3
        
        pids = pid.split(',')
        
        if pids != ['']: #dft
            stats = BkrackMsg.mgr().get_all_stat_multi_pids(start,tody,pids)
        else:    
            stats = BkrackMsg.mgr().get_all_stat(start,tody)
        for stat in stats:
            #version_name
            version_name_list = stat['version_name'].split(',')
            version_len = len(version_name_list)
            if version_len > MAX_PARTNER_SHOW:
                stat['version_name'] = ','.join(version_name_list[0:3])
            #channel_id
            partner_list = stat['channel_id'].split(',')
            partner_list_all = partner_list
            if len(partner_list) > MAX_PARTNER_SHOW:
                partner_list = partner_list[0:3]
                partner_list.append('...')
            stat['channel_id'] = ''
            stat['channel_id_all'] = ''
            for partner in partner_list:
                stat['channel_id'] = stat['channel_id'] + str(partner)+','
            for partner in partner_list_all:
                stat['channel_id_all'] = stat['channel_id_all'] + str(partner) + ','
            stat['channel_id'] = stat['channel_id'].strip(',')
            stat['channel_id_all'] = stat['channel_id_all'].strip(',')
            #time
            stat['start_time'] = stat['start_time'].strftime('%Y-%m-%d')
            stat['end_time'] = stat['end_time'].strftime('%Y-%m-%d')
            #url
            if stat['msg_url'] == None:
                stat['msg_url'] = ''
        #get push user
        stats = self.get_push_user(stats,ds)
        #get pv uv
        stats = self.get_pv_uv(stats,yest)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/push_info.html',pid=pid,
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def push_sum(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        stat1 = list(PustStat.mgr().get_daily_sum_stat(start,end)[:]) 
        stat2 = list(PustStat.mgr().get_daily_stat(start,end,False)[:]) #pid != ''
        dss = []
        for i in stat1:
            dss.append(i['ds'])
        dss2 = []
        for i in stat2:
            dss2.append(i['ds'])

        for ds in dss:
            if ds not in dss2:
                stat = {'uv': 0, 'pv': 0, 'ds': ds, 'user_visit':0}
                stat2.append(stat)
        stat2 = self.bubblesort_asc(stat2,'ds')
        
        #merge
        for i in stat1:
            for j in stat2:
                if i['ds'] == j['ds']:
                    i['user_visit_pid'] = j['user_visit']
            if i['pv'] == None:
                i['pv'] = 0
            if i['uv'] == None:
                i['uv'] = 0
        stats = stat1
        #get pid
        stats = PustStat.mgr().get_pid(stats)
        for stat in stats:
            if stat['pids'] == '':
                stat['pids'] = 'null'
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/push_sum.html',
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def push_detail(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        pid = int(self.get_argument('pid',0))
        channel = int(self.get_argument('channel',0))
        innerVer = int(self.get_argument('innerVer',0))
        order_field = self.get_argument('orderField','')

        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = PustStat.mgr().get_all_stat(start,end)
        if pid != 0:
            stats = stats.filter(pid=pid)
        if channel != 0:
            stats = stats.filter(channel=channel)
        if innerVer != 0:
            stats = stats.filter(innerVer=innerVer)

        for stat in stats:
            if stat['pv'] == None:
                stat['pv'] = 0
            if stat['uv'] == None:
                stat['uv'] = 0
            if stat['pid'] == None:
                stat['pid'] = ''
            if stat['innerVer']:
               stat['innerVer'] = PUSH_VERSION.get(int(stat['innerVer']))

        #acc
        acc = {'ds':'总和','pid':'','channel':'','innerVer':'','pv': 0, 'uv': 0, 'visits': 0, 'user_visit': 0}
        for stat in stats:
            acc['pv'] += stat['pv']
            acc['uv'] += stat['uv']
            acc['visits'] += stat['visits']
            acc['user_visit'] += stat['user_visit']
        stats = stats[:]
        if order_field != '':
            stats = self.bubblesort(stats,order_field)
        stats.append(acc)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/push_detail.html',pid=pid,channel=channel,innerVer=innerVer,order_field=order_field,
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def push_partner_detail(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        pid = self.get_argument('pid',0)
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=120)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        MAX_PARTNER_SHOW_DETAIL = 2000

        stats = BkrackMsg.mgr().get_all_stat(start,end)
        if pid != 0:
            stats = stats.filter(id=pid)
        for stat in stats:
            #channel_id
            partner_list = stat['channel_id'].split(',')
            if len(partner_list) > MAX_PARTNER_SHOW_DETAIL:
                partner_list = partner_list[0:3]
                partner_list.append('...')
            stat['channel_id'] = ''
            for partner in partner_list:
                stat['channel_id'] = stat['channel_id'] + str(partner)+','
            stat['channel_id'] = stat['channel_id'].strip(',')
            #time
            stat['start_time'] = stat['start_time'].strftime('%Y-%m-%d')
            stat['end_time'] = stat['end_time'].strftime('%Y-%m-%d')
            #url
            if stat['msg_url'] == None:
                stat['msg_url'] = ''

        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/push_partner_detail.html',pid=pid,
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def recommendation(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        appid = self.get_argument('appid','')
        action = self.get_argument('action','')
        type = self.get_argument('type','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        
        stats = []
        _start,days = start,[]
        while _start <= end:
            _end = _start + datetime.timedelta(days=1)
            stat, appids = Recommendation.mgr().get_recommendation_one_day_stat(_start,appid)
            stat = self.data_process(stat,appids,_start)
            stat = self.bubblesort(stat,'download')
            stats.extend(stat)
            days.append(_start)
            _start = _end

        for stat in stats:
            stat['time'] = stat['time'].strftime('%Y-%m-%d')
        stats = Service.inst().fill_app_info(stats) 
        count = len(stats) 
        if action == 'export':
            title = [('time','时间'),('appid','appid'),('appname','app名字'),('download','下载'),('install','安装')]
            xls = Excel().generate(title,stats,1)
            filename = 'recommendation_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/recommendation.html',appid=appid,type=type,
            date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def qiandao_level(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        action = self.get_argument('action','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = Qiandao.mgr().get_all_stat(start,end)
        if action == 'export':
            title = [('ds','时间'),('level','等级'),('card6','6阅饼'),('card11','11阅饼'),('card16','16阅饼'),('card21','21阅饼'),('card26','26阅饼'),('card52','52阅饼')]
            xls = Excel().generate(title,stats,1)
            filename = 'qiandao_level_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            count = len(stats)
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/qiandao_level.html',
                date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def qiandao_basic(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        action = self.get_argument('action','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = QiandaoBasic.mgr().get_all_stat(start,end)
        if action == 'export':
            title = [('ds','日期'),('qiandaonum','签到人数'),('buqiannum1','补签请求人数'),('buqiannum2','补签成功人数'),('bangdingnum','绑定手机号'),('mergenum','合并账户')]
            xls = Excel().generate(title,stats,1)
            filename = 'qiandao_basic_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            count = len(stats)
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/qiandao_basic.html',
                date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def qiandao_recharge(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        action = self.get_argument('action','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = QiandaoRecharge.mgr().get_all_stat(start,end)
        if action == 'export':
            title = [('ds','时间'),('rechargetype','充值类型'),('num','数量'),('amount','金额')]
            xls = Excel().generate(title,stats,1)
            filename = 'qiandao_basic_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            count = len(stats)
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/qiandao_recharge.html',
                date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def pvuv_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        action = self.get_argument('action','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        key = self.get_argument('key','') 
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = A0.mgr().get_pv_uv_multi_days(start,end)
        if key:
            stats = stats.filter(a0=key)
        for stat in stats:
            stat['time'] = stat['time'].strftime('%Y-%m-%d')
        if action == 'export':
            title = [('time','日期'),('a0','KEY'),('pv','PV'),('uv','UV')]
            xls = Excel().generate(title,stats,1)
            filename = 'qiandao_basic_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            count = len(stats)
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/pvuv_stat.html',key=key,
                date=yest.strftime('%Y-%m-%d'),start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def baoyue_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        partner_id = int(self.get_argument('partner_id',0))
        version_name = self.get_argument('version_name','')
        action = self.get_argument('action','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        count,stats = 0,[]
        stats = OperaBaoyueSum.mgr().get_all_stat(start,end)
        if partner_id:
            stats = stats.filter(partner_id=partner_id)
        if version_name:
            stats = stats.filter(versionname=version_name)
        if action == 'export':
            title = [('ds','日期'),('partner_id','渠道'),('versionname','版本'),('orderpv','订购次数'),('orderuv','订购人数'),('renewtimes','续订次数'),
                ('rechargingnum','主账号消费'),('giftrechargingnum','副账号消费')]
            xls = Excel().generate(title,stats,1)
            filename = 'qiandao_basic_%s.xls' % (yest.strftime('%Y-%m-%d'))
            self.set_header('Content-Disposition','attachment;filename=%s'%filename)
            self.finish(xls)
        else:
            count = len(stats)
            stats = stats[(page-1)*psize:page*psize]
            self.render('data/baoyue_stat.html',partner_id=partner_id,version_name=version_name,date=yest.strftime('%Y-%m-%d'),
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def baoyue_sum_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=7)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        delta = tody - start
        count,stats = 0,[]
        days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
        res = []
        acc =  {'giftrechargingnum': 0, 'orderuv': 0, 'renewtimes': 0, 'orderpv': 0, 'ds': '总和', 'rechargingnum': 0}
        for i in days:
            stats = OperaBaoyueSum.mgr().get_one_day_stat(i)
            if stats[0]['ds']:
                acc['giftrechargingnum'] += stats[0]['giftrechargingnum']
                acc['orderuv'] += stats[0]['orderuv']
                acc['renewtimes'] += stats[0]['renewtimes']
                acc['orderpv'] += stats[0]['orderpv']
                acc['rechargingnum'] += stats[0]['rechargingnum']
                res.append(stats[0])
        res.append(acc)
        count = len(res)
        res = res[(page-1)*psize:page*psize]
        self.render('data/baoyue_sum_stat.html',date=yest.strftime('%Y-%m-%d'),
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,res=res)

    def baoyue_book_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        book_id = self.get_argument('book_id','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=1)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        delta = tody - start
        #days = [start+datetime.timedelta(days=i) for i in range(delta.days)]
        books = []
        #for i in days:
        #    stats = OperaBaoyueBook.mgr().get_one_day_stat(i,book_id)
        #    books.append(stats)
        #if len(books) > 1: # for multi days 
        #    res = self.operation_merge_multi_days_books(books)
        #else:
        #    res = list(books[0])
        #booid->book_id
        stats = OperaBaoyueBook.mgr().get_stat(start,end,book_id)
        res = stats 
        for i in res:
            i['book_id'] = int(i['bookid'])
        res = Service.inst().fill_book_info(res)
        count = len(res)
        res = res[(page-1)*psize:page*psize]
        self.render('data/baoyue_book_stat.html',date=yest.strftime('%Y-%m-%d'),book_id=book_id,
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,res=res)

    def txt_recommendation(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        book_id = self.get_argument('book_id','')
        order_field = self.get_argument('orderField','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=1)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
        stats = TxtRecommendation.mgr().get_stat(start,end,order_field,book_id) 
        stats = Service.inst().fill_book_info(stats)
        for i in stats:
            i['time'] = i['time'].strftime('%Y-%m-%d')
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/txt_recommendation.html',date=yest.strftime('%Y-%m-%d'),book_id=book_id,order_field=order_field,
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def pop_window_stat(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        book_id = self.get_argument('book_id','')
        type = self.get_argument('type','')
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=1)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
           
        stats = OperateCenter.mgr().get_operate_center_stat(start,end)
        if book_id:
            stats = stats.filter(bookid=book_id)
        if type:
            stats = stats.filter(type=type)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/pop_window_stat.html',date=yest.strftime('%Y-%m-%d'),book_id=book_id,type=type,
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def vp_custom(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=1)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
          
        stats = VPCustom.mgr().get_vp_stat(start,end)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/vp_custom.html',date=yest.strftime('%Y-%m-%d'),
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)

    def vp_cancel(self):
        page = int(self.get_argument('pageNum',1))
        psize = int(self.get_argument('numPerPage',100))
        tody = self.get_date(1) + datetime.timedelta(days=1)
        start = self.get_argument('start','')
        yest = tody - datetime.timedelta(days=1)
        if not start:
            start = tody - datetime.timedelta(days=1)
        else:
            start = datetime.datetime.strptime(start,'%Y-%m-%d')
        end = yest
          
        stats = VPCancel.mgr().get_vp_stat(start,end)
        count = len(stats)
        stats = stats[(page-1)*psize:page*psize]
        self.render('data/vp_cancel.html',date=yest.strftime('%Y-%m-%d'),
            start=start.strftime('%Y-%m-%d'),page=page,psize=psize,count=count,stats=stats)








