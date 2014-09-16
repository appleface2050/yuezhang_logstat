#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from model.stat import Scope,BasicStat,VisitStat,TopNStat,BookStat,ProductStat,BookCategory
from model.factory import Factory,Partner
from service import Service

def insert_database(cate0,cate1,cate2):
    try:
        f = BookCategory.new()
        f.cate0 = cate0
        f.cate1 = cate1
        f.cate2 = cate2
        f.save()
        return True
    except Exception, e:
        print e
        return False


if __name__ == "__main__":
    insert_database('男频','玄幻奇幻','东方玄幻')
    insert_database('男频','玄幻奇幻','异世大陆')
    insert_database('男频','玄幻奇幻','王朝争霸')
    insert_database('男频','玄幻奇幻','远古神话')
    insert_database('男频','玄幻奇幻','西方奇幻')
    insert_database('男频','玄幻奇幻','魔法校园')
    insert_database('男频','武侠仙侠','传统武侠')
    insert_database('男频','武侠仙侠','浪子异侠')
    insert_database('男频','武侠仙侠','古典仙侠')
    insert_database('男频','武侠仙侠','奇幻修真')
    insert_database('男频','武侠仙侠','现代修真')
    insert_database('男频','都市生活','都市暧昧')
    insert_database('男频','都市生活','都市重生')
    insert_database('男频','都市生活','都市异能')
    insert_database('男频','都市生活','职场商场')
    insert_database('男频','都市生活','宦海沉浮')
    insert_database('男频','都市生活','黑道风云')
    insert_database('男频','都市生活','乡土小说')
    insert_database('男频','历史军事','架空历史')
    insert_database('男频','历史军事','历史传记')
    insert_database('男频','历史军事','战争幻想')
    insert_database('男频','历史军事','现代军事')
    insert_database('男频','游戏竞技','虚拟网游')
    insert_database('男频','游戏竞技','电子竞技')
    insert_database('男频','游戏竞技','体育竞技')
    insert_database('男频','科幻同人','未来世界')
    insert_database('男频','科幻同人','末世危机')
    insert_database('男频','科幻同人','武侠同人')
    insert_database('女频','现代言情','总裁豪门')
    insert_database('女频','现代言情','白领职场')
    insert_database('女频','现代言情','军旅高干')
    insert_database('女频','现代言情','都市纯爱')
    insert_database('女频','现代言情','女性网游')
    insert_database('女频','现代言情','重生异能')
    insert_database('女频','古代言情','宫斗宅斗')
    insert_database('女频','古代言情','穿越架空')
    insert_database('女频','古代言情','家长里短')
    insert_database('女频','古代言情','女尊女强')
    insert_database('女频','古代言情','古色古香')
    insert_database('女频','耽美同人','古代耽美')
    insert_database('女频','耽美同人','现代耽美')
    insert_database('女频','耽美同人','同人小说')
    insert_database('女频','魔幻情缘','仙侠幻情')
    insert_database('女频','魔幻情缘','妖精情缘')
    insert_database('女频','魔幻情缘','魔法异界')
    insert_database('女频','魔幻情缘','异国情缘')
    insert_database('女频','青春校园','台湾言情')
    insert_database('女频','青春校园','校园纯爱')
    insert_database('女频','青春校园','黑帮迷情')
    insert_database('女频','悬疑灵异','悬疑推理')
    insert_database('女频','悬疑灵异','恐怖惊悚')
    insert_database('女频','悬疑灵异','灵异神怪')
    insert_database('女频','悬疑灵异','盗墓探险')
    insert_database('出版分类','名著传记','世界名著')
    insert_database('出版分类','名著传记','古典小说')
    insert_database('出版分类','名著传记','近现代小说')
    insert_database('出版分类','名著传记','人物传记')
    insert_database('出版分类','名著传记','国学经典')
    insert_database('出版分类','名著传记','诗词歌赋')
    insert_database('出版分类','名著传记','名著其他')
    insert_database('出版分类','经管励志','心理励志')
    insert_database('出版分类','经管励志','投资理财')
    insert_database('出版分类','经管励志','职场管理')
    insert_database('出版分类','经管励志','市场营销')
    insert_database('出版分类','经管励志','商务实务')
    insert_database('出版分类','经管励志','经济金融')
    insert_database('出版分类','经管励志','演讲口才')
    insert_database('出版分类','畅销小说','青春小说')
    insert_database('出版分类','畅销小说','都市小说')
    insert_database('出版分类','畅销小说','言情小说')
    insert_database('出版分类','畅销小说','乡土小说(出)')
    insert_database('出版分类','畅销小说','军事小说')
    insert_database('出版分类','畅销小说','历史小说')
    insert_database('出版分类','畅销小说','科幻小说')
    insert_database('出版分类','畅销小说','推理小说')
    insert_database('出版分类','畅销小说','灵异小说')
    insert_database('出版分类','畅销小说','影视小说')
    insert_database('出版分类','畅销小说','官场小说')
    insert_database('出版分类','畅销小说','职场小说')
    insert_database('出版分类','生活时尚','旅游美食')
    insert_database('出版分类','生活时尚','服装服饰')
    insert_database('出版分类','生活时尚','健康养生')
    insert_database('出版分类','生活时尚','美容瘦身')
    insert_database('出版分类','生活时尚','星座血型')
    insert_database('出版分类','人文社科','历史军事')
    insert_database('出版分类','人文社科','政治法律')
    insert_database('出版分类','人文社科','宗教哲学')
    insert_database('出版分类','人文社科','社科其他')
    insert_database('出版分类','人文社科','名家文集')
    insert_database('出版分类','人文社科','散文随笔')
    insert_database('出版分类','人文社科','纪实文学')
    insert_database('出版分类','教育教辅','工具书')
    insert_database('出版分类','教育教辅','教育其他')
    insert_database('出版分类','少儿读物','儿童文学')
    insert_database('出版分类','少儿读物','少儿其他')
    insert_database('出版分类','短篇小品','杂志期刊')
    insert_database('出版分类','短篇小品','经典语录')
    insert_database('出版分类','短篇小品','极品笑话')
    insert_database('出版分类','短篇小品','开心短信')
    insert_database('出版分类','短篇小品','爆笑网文')








