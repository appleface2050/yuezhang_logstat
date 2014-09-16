#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import base64
import time
import datetime

def strptime(dtime, format):
    """
    for < 2.6
    """
    time_stamp = time.mktime(time.strptime(dtime,format))
    return datetime.datetime.fromtimestamp(time_stamp)

def time_start(d, typ):
    """
    """
    if typ == "hour":
        d -= datetime.timedelta(minutes=d.minute,seconds=d.second,microseconds=d.microsecond)
    elif typ == "day":
        d -= datetime.timedelta(hours=d.hour,minutes=d.minute,seconds=d.second,microseconds=d.microsecond)
    elif typ == "week":
        d -= datetime.timedelta(days=d.weekday(),hours=d.hour,minutes=d.minute,seconds=d.second,microseconds=d.microsecond)
    elif typ == "month":
        d -= datetime.timedelta(days=d.day-1,hours=d.hour,minutes=d.minute,seconds=d.second,microseconds=d.microsecond)
    else:
        raise Exception("wrong type %s" % (typ,))
    return d

def time_next(d, typ):
    if typ == "hour":
        d += datetime.timedelta(hours=1)
    elif typ == "day":
        d += datetime.timedelta(days=1)
    elif typ == "week":
        d += datetime.timedelta(days=7)
    elif typ == "month":
        year = d.year+1 if d.month==12 else d.year
        month = 1 if d.month==12 else d.month+1
        d = datetime.datetime(year,month,1)
    else:
        raise Exception("wrong type %s" % (typ,))
    return time_start(d,typ)
    
def strftime_day(dtime):
    """
    for simplariy
    """
    return str(time_start(dtime,"day"))

def gzip_compress(data):
    zbuf = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb',compresslevel=9,fileobj=zbuf)
    zfile.write(data)
    zfile.close()
    return zbuf.getvalue()

def gzip_decompress(data):
    zbuf = StringIO.StringIO(data)
    zfile = gzip.GzipFile(fileobj=zbuf)
    data = zfile.read()
    zfile.close()
    return data

