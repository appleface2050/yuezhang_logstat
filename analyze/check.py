#!/usr/bin/env python
# -*- coding : utf-8 -*-

import sys
import MySQLdb

def check(date):
    conn = MySQLdb.connect(host='192.168.0.227',user='analy',passwd='analy123',db='logstat')

    cursor = conn.cursor()
    #sql = "select count(1) cnt from dm_v6_search where sumdate='%s'" % date

    rdict = []
    for table in ['dm_v6_visit', 'dm_v6_briefvisit', 'dm_v6_download', 'dm_v6_down_recharge', 'dm_v6_search']:
        sql = "select  count(1) cnt from %s where sumdate='%s'" % (table, date)
        cursor.execute(sql)
        lines = cursor.fetchall()
        if lines:
            for line in lines:
                count = int(line[0])
                if count == 0:
                    rdict.append(table)
    conn.close()
    return rdict

if __name__ == "__main__":
    date = sys.argv[1]
    res = check(date)
    if len(res) == 0:
        sys.exit(0)
    else:
        print res
        sys.exit(1)
