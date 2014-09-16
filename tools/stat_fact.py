#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat
from model.factory import Factory,Partner
from service import Service


def compute_day(factory_dict, result, day):
    print 'start...',day
    start,end = day,day+datetime.timedelta(days=1)
    all_stats = Service.inst().stat.get_all_factstat('day',start,end)
    acc_stats = {}
    for u in factory_dict:
        for f in factory_dict[u]:
            basic = all_stats.get(f.id,None)
            if basic:
                if u in result:
                    result[u] += basic['new_user_run']
                else:
                    result[u] = basic['new_user_run']
    print 'end...',day

if __name__ == "__main__":
    factory_dict = {}
    factory_dict['高兵'] = Factory.mgr().Q().filter(group='高兵').extra("name<>'OPPO智能' AND name<>'OPPO市场'").data()
    factory_dict['廖凯'] = Factory.mgr().Q().filter(group='廖凯').data()
    factory_dict['柳成峰'] = Factory.mgr().Q().filter(group='柳成峰').data()
    factory_dict['贾生亭'] = Factory.mgr().Q().filter(group='贾生亭').data()
    factory_dict['高猛'] = Factory.mgr().Q().filter(group='高猛').data()
    factory_dict['张夏'] = Factory.mgr().Q().filter(group='张夏').data()
    factory_dict['陈少雄'] = Factory.mgr().Q().filter(group='陈少雄').data()
    factory_dict['张婧'] = Factory.mgr().Q().filter(group='张婧').data()
    factory_dict['徐超'] = Factory.mgr().Q().filter(group='徐超').data()
    factory_dict['王超'] = Factory.mgr().Q().filter(group='王超').data()
    factory_dict['吴伟'] = Factory.mgr().Q().filter(group='吴伟').data()
    factory_dict['贾怀青'] = Factory.mgr().Q().filter(group='贾怀青').data()
    factory_dict['OPPO智能'] = Factory.mgr().Q().filter(name='OPPO智能').data()
    factory_dict['OPPO市场'] = Factory.mgr().Q().filter(name='OPPO市场').data()
    result = {}
    #在这里改时间
    start = datetime.datetime(2014,4,1)
    end = datetime.datetime(2014,5,1)
    while start < end:
        compute_day(factory_dict,result,start)
        start += datetime.timedelta(days=1)
    for i in result:
        print '%s\t%s' % (i,result[i])
#
#SELECT COUNT(DISTINCT(user)),last_model FROM userset_init_pool WHERE (first_channel like '108%' OR first_channel like '109%' OR first_channel like '110%' OR first_channel like '111%') AND ds>='2013-10-07' AND ds<'2013-10-08' AND firstinittime>='2013-09-01' and firstinittime<'2013-10-01' and UPPER(last_model) like '%VIVO%' group by last_model



