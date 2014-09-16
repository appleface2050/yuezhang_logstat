# -*- coding: utf-8 -*-
# Abstract: settings

import json

# mysql
SDB = {
	'host':'192.168.0.168',
	'user':'opm',
	'passwd':'operationm_pwd',
	'db':'',
	'sock':'',
	'port':3356
}

MDB = {
    'host':'192.168.0.227',
    'user':'analy',
    'passwd':'analy123',
    'db':'',
    'sock':'/var/zhangyue/mysql/s_mysql.sock',
    'port':3306
}

RECHARGE_LOG_SDB = {
	'host':'192.168.0.161',
	'user':'ebk6',
	'passwd':'zzhy13579',
	'db':'',
	'sock':'',
	'port':3316
}

MONITOR_DATA_SDB = {
	'host':'192.168.0.150',
	'user':'analy',
	'passwd':'test1357',
	'db':'',
	'sock':'',
	'port':3316
}

EBK5_BOOK_EXPAND = {
    'host':'192.168.0.118',
    'port':3306,
    'user':'zydev',
    'passwd':'zzhy+2468!#!D5',
    'db':'',
    'sock':''
}

DB_CNF = {
	'm':{json.dumps(MDB):['logstat','system','week_ana']},
	's':{json.dumps(SDB):['logstat','system','week_ana'],json.dumps(RECHARGE_LOG_SDB):['book'],json.dumps(MONITOR_DATA_SDB):['dw_v6_test'],
    json.dumps(EBK5_BOOK_EXPAND):['polarisebk5']},
}

DB_TEST = {
    'm':{json.dumps(MDB):['logstat', 'logstatV2','week_ana','external','logstatv3','datamining','payment_v6'], json.dumps(EBK5_BOOK_EXPAND):['polarisebk5']},
    's':{json.dumps(SDB):['logstat', 'logstatV2','week_ana','logstatv3','payment_v6'], 
        json.dumps(RECHARGE_LOG_SDB):['book'],
        json.dumps(MONITOR_DATA_SDB):['dw_v6_test'],
        json.dumps(EBK5_BOOK_EXPAND):['polarisebk5'],
        json.dumps(MDB):['datamining','watchdog','external']
    },
}

# hive
HiveConf = {
    'host':'192.168.0.144',
    'port':10000
}

# book plan api
BOOK_API = 'http://192.168.0.208:27200/book'
PLAN_API = 'http://192.168.0.208:27200/scheme'
SUBJECT_API = 'http://192.168.0.208:27200/subject'

BOOK_wordCount_chapterCount_API = 'http://192.168.0.220:8070/cps/getBookDetail?fid=41&p5=19&bookId=' #wordCount chapterCount

# session conf
MC_SERVERS = ["192.168.0.150:11000"]
COOKIE_NAME = 'session_id'
COOKIE_SECRET= 'FPdaUI5QAGaDdkL5gEmGeJJFuYh7EQnp2XdTP1'
SESSION_USER = 'user'
KEY_PRE = 'zy'


# conf for scope
STAT_MASK = {'basic':'基本统计','visit':'页面分析','topn':'topN统计','book':'书籍分析','product':'厂商分析'}
STAT_MODE = {'day':'日','week':'周','month':'月'}

# conf for 页面类型  topN类型
PAGE_TYPE = {'chosen':([1],'book.php','精选'),
             'pub':([2],'book.php','出版'),
             'boy':([3],'book.php','男频'),
             'girl':([4],'book.php','女频'),
             'freebytime':([5],'book.php','限免'),
             'category':([6],'book.php','分类'),
             'rank':([7],'book.php','排行'),
             'ucenter':([8],'user.php','用户中心'),
             'recharge':([10],'recharging.php','充值'),
             'privcenter':([9],'user.php','特权中心'),
             'search':([11],'search.php','搜索'),
             'bkintro':([12],'book.php','简介')}

TOP_TYPE = {'board':('13','pagekey','榜单'),
            'tag':('14','pagekey','标签'),
            'search':('2','search_key','搜索'),
            'hotword':('3','search_key','热词'),
            'subject':('15','pagekey','专题')}

# conf for wap page type
WAP_PAGE_TYPE = {'mainpage':'精选',
                'publish_page':'出版',
                'male':'男频',
                'female':'女频',
                'classification':'分类',
                'ranking':'排行',
                'search_main':'搜索',
                'user_entry':'个人中心',
                'charge':'充值',
                'bookshelf':'书架'}

# report_v5_monitoring_data
REPORT_V5_MONITOR = {1:'MTK方案',
                    7:'朵唯女性内容方案',
                    8:'MTK只能版本方案',
                    14:'MTK测试版',
                    21:'塞班S60-老方案',
                    22:'塞班S60-新方案',
                    24:'iReader内置收费方案',
                    26:'Kjava-S40新方案',
                    27:'iReader实体书方案',
                    28:'iPhone静态方案v2.2',
                    34:'塞班S6动态收费方案',
                    37:'MTK小屏幕方案',
                    38:'MTK大屏幕方案',
                    41:'iReader动态按章计费方案',
                    42:'The-New_iPad动态方案',
                    47:'iReader金立THL方案',
                    49:'iPhone动态v2.3',
                    50:'WP7动态V1.1.0'}

REPORT_V5_MONITOR_REVERSE = {'MTK方案':1,
                    '朵唯女性内容方案':7,
                    'MTK只能版本方案':8,
                    'MTK测试版':14,
                    '塞班S60-老方案':21,
                    '塞班S60-新方案':22,
                    'iReader内置收费方案':24,
                    'Kjava-S40新方案':26,
                    'iReader实体书方案':27,
                    'iPhone静态方案v2.2':28,
                    '塞班S6动态收费方案':34,
                    'MTK小屏幕方案':37,
                    'MTK大屏幕方案':38,
                    'iReader动态按章计费方案':41,
                    'The-New_iPad动态方案':42,
                    'iReader金立THL方案':47,
                    'iPhone动态v2.3':49,
                    'WP7动态V1.1.0':50} 

#basic
WAP_PAGE_TYPE_BASIC ={'mainpage':'精选',
                    'publish_page':'出版',
                    'male':'男频',
                    'female':'女频',
                    'classification':'分类',
                    'ranking':'排行',
                    'search_main':'搜索',
                    'user_entry':'个人中心',
                    'charge':'充值',
                    'bookshelf':'书架'}

#use to update the zero new_user_run to the new_visit_run stat
#some partner don't have 1T1 ,so the new_user_run is zero,                                                                                     
#but the old Algorithm from weimin's userbaisc_v6 did have a number which is the new_visit_run,
#I have to update this stat to make the factory and business man who has the factory HAPPY.
SHOULD_UPDATE_ZERO_NEW_USER_RUN_PARTNER_LIST = [110010]

#use for factstat recharge log query count limit
Query_Limit_Count = 1000

#for wenyao's PUSH VERSION
PUSH_VERSION = {
    5720:"ireader_1.6.2",
    5730:"ireader_1.7.0",
    5740:"ireader_1.7.1",
    5741:"ireader_1.7.2",
    5800:"ireader_1.8.0",
    5810:"ireader_1.8.1",
    6000:"ireader_2.0.0",
    6100:"ireader_2.1.0",
    6101:"ireader_2.1.1",
    6200:"ireader_2.2.0",
    5743:"ireader_1.7.3",
    6300:"ireader_2.3.0",
    5710:"ireader_1.6.1",
    6310:"ireader_2.3.1",
    6600:"ireader_2.6.0",
    6700:"ireader_2.7.0",
    6230:"ireader_2.2.3"
}


#for API
API_TRY_TIME_LIMIT = 240

#for logstat data error email
ERROR_EMAIL_MAILTO_LIST = ["shangzongkai@zhangyue.com","wangchunyang@zhangyue.com","shanjianguo@zhangyue.com","appleface2050@qq.com"]
ERROR_EMAIL_SUBJECT = "ERROR!!! logstat data break down"
ERROR_EMAIL_HTML="OMG!!! logstat data break down \n {{{(>_<)}}} \n\n\n\n\n\n\n\n\n\n\n\n\n auto email don\'t reply"

#for monthly business 
MONTHLY = ["2013-01","2013-02","2013-03","2013-04","2013-05","2013-06"
            ,"2013-07","2013-08","2013-09","2013-10","2013-11","2013-12","2014-01","2014-02","2014-03",
            "2014-04"]

#IOS run ID
IOS_RUN_ID_LIST = [501607,501621,501631,501632]

#Safe user Who can see all factory's accouting data
#This is bad, but I have no time to create a better way, may be make this better some time not busy
SAFE_USER = ['admin','wangliang_acc','wangliang']

#some partner are special should not update new user run
DO_NOT_UPDATE_NEW_USER_RUN_PARTNERID_LIST = [109180,109175]

#Special factory Coefficient
SPECIAL_FACTORY_COEFFICIENT = {229:{'starttime':'2014-01-01','coefficient':1.333333},
                               2144:{'starttime':'2014-01-01','coefficient':0.888888},
                                }

#Special factory Coefficient 2 
SPECIAL_FACTORY_COEFFICIENT2 = {229:{'starttime':'2014-02-01','coefficient':1.25}, #0.4->0.5
                               2144:{'starttime':'2014-02-01','coefficient':0.875}, #0.8->0.7
                                }

#accounting factory title
ACCOUNTING_FACTORY_TITLE = {
    'user_run':'启动ireader的用户数',
    'new_user_run':'首次启动ireader的用户数',
    'pay_user':'购买付费书籍的用户数',
    'cpay_down':'按章计费付费书籍下载次数',
    'bpay_down':'按本计费付费书籍下载次数',
    'cpay_user':'购买按章计费付费书籍的用户数',
    'bpay_user':'购买按本计费付费书籍的用户数',
    'cfree_down':'按章免费书籍下载次数',
    'bfree_down':'按本免费书籍下载次数',
    'cfree_user':'购买按章计费免费书籍的用户数',
    'bfree_user':'购买按本计费免费书籍的用户数',
    'feesum':'预期收入'
}


#caiwenyan demand
DIR_CWY_POPIN_RESPONSE = "/var/userapps/caiwenyan/popin_window/popin_response.txt"  
DIR_CWY_POPIN_DOWNLOAD_AMOUNT = "/var/userapps/caiwenyan/popin_window/popin_download_amount.txt"
DIR_CWY_NONWIFI_POPIN_DOWNLOAD = "/var/userapps/caiwenyan/popin_window/nonwifi_popin_download.txt"









