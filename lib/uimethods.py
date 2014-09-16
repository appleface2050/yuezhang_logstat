#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime

def strftime_day(handler, dtime):
    return dtime.strftime('%Y-%m-%d')
    
def jsondump(handler, obj):
	import json
	return json.dumps(obj)

def base64_url(handler, url):
	import base64
	return base64.b64encode(url)

def extract_url(handler, url):
	return url.split("http://")[1].split("/")[0]

def formatdate(handler, obj=None, format="%Y-%m-%d %H:%M:%S"):
	from datetime import datetime
	res = datetime.now() if not obj else obj
	return res.strftime(format)

def number_parity(handler, number):
	# check a number is an odd number or an even number
	if number % 2 == 0:
		return 'odd'
	else:
		return 'even'

def stc_url(handler, uri):
	return 'stc/%s'%uri

def touch_resource(handler, args):
    if handler.current_user:
        return handler.current_user.touch_resource(args)
    return False

def has_perm(handler, oper, resource, **attr):
    if handler.current_user:
        return handler.current_user.has_perm(oper,resource,**attr)
    return False

