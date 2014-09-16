#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# Abstract: start split v6 log

import os
import sys
import getopt
import datetime
import logging
import fcntl
import errno

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start

class SplitLog(object):
	'''
	split service&download v6 log
	'''
	HIVE_HOME ="/user/hive/warehouse"
	HIVE = '/usr/local/hive-0.7.1-cdh3u3/bin/hive'	
	HADOOP = '/usr/local/hadoop-0.20.2-cdh3u3/bin/hadoop'
	
	def __init__(self, logname='splitting.log'):
		self.logger	= self.get_logger(os.path.join(SplitLog.get_cur_dir(),logname))

	@staticmethod
	def get_logger(filename):
		"""
		logger to write my msgs
		"""
		logger = logging.getLogger(os.path.split(filename)[1])
		hdlr = logging.FileHandler(filename)
		formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
		hdlr.setFormatter(formatter)
		logger.addHandler(hdlr)
		logger.setLevel(logging.INFO)
		return logger

	@staticmethod
	def lock(fd):
		try:
			fcntl.lockf(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
		except IOError, e:
			if e.errno in (errno.EACCES, errno.EAGAIN):
				self.logger.error("There is an instance of %s running. Quit"%sys.argv[0],exc_info=True)
				sys.exit(0)
			else:
				raise

	@staticmethod
	def get_cur_dir():
		cur = os.path.realpath(__file__)
		return os.path.dirname(cur)
		
	@staticmethod
	def exec_sys_cmd(cmd):
		res = None
		try:
			f = os.popen('. /etc/profile;'+cmd)	
			res = f.read()
			f.close()
		except Exception,e:
			self.logger.error('%s\n',str(e),exc_info=True)
		print 'exec_sys_cmd-->%s==>%s'%(cmd,res)
		return res

	def process(self, job_type, hour_time=None):
		assert job_type in ('download_v6','service_v6')
		if not hour_time:
			hour_time = datetime.datetime.now() - datetime.timedelta(hours=1)
		day = hour_time.strftime('%Y%m%d')
		hour = hour_time.strftime('%H')

		# add partition
		if hour_time.hour == 0:
			cmd = "alter table %s add partition (ds='%s') location '%s/%s/ds=%s';"%(job_type,day,self.HIVE_HOME,job_type,day)
			cmd = '%s -S -e "%s"'%(self.HIVE,cmd)
			res = SplitLog.exec_sys_cmd(cmd)

		# filter & formalization
		input_file = '%s/%s/ds=%s/%s'%(self.HIVE_HOME,'DOWNLOAD_LOG_V6' if job_type=='download_v6' else 'SERVICES_LOG_V6', day,hour)
		output_dir = '%s/%s/ds=%s'%(self.HIVE_HOME,job_type,day)
		curdir = SplitLog.get_cur_dir()
		split_cmd = '%s jar %s/split_log_v6.jar %s %s %s:%s:%s'%(self.HADOOP,curdir,input_file,output_dir,job_type,day,hour)
		#split_cmd = '%s jar %s/split_log_v6_szk.jar %s %s %s:%s:%s'%(self.HADOOP,curdir,input_file,output_dir,job_type,day,hour)
		res = SplitLog.exec_sys_cmd(split_cmd)

		# load data
		#cmd = "LOAD DATA INPATH '%s/%s' INTO TABLE %s PARTITION (ds='%s');"%(output_dir,hour,job_type,day)
		#cmd = '%s -S -e "%s"'%(self.HIVE,cmd)
		#res = SplitLog.exec_sys_cmd(cmd)

		return True

if __name__ == "__main__":
	try:
		opts, args = getopt.getopt(sys.argv[1:],'',['jobtype=','start=','end=','mode='])
	except getopt.GetoptError,e:
		logging.error('%s\n',str(e),exc_info=True)
		sys.exit(2)
	jobtype,start,end,mode = '',None,None,'hour'
	for o, a in opts:
		if o == '--jobtype':
			jobtype = a
		if o == '--start':
			start = datetime.datetime.strptime(a,'%Y-%m-%d:%H')
		if o == '--end':
			end = datetime.datetime.strptime(a,'%Y-%m-%d:%H')
		if o == '--mode':
			mode = a
	assert jobtype in ('download_v6','service_v6')
	assert mode in ('hour','day')
	lock_file_path = '/tmp/split_%s_%s_%s_%s.lock'%(jobtype,start,end,mode)
	fd = os.open(lock_file_path, os.O_CREAT|os.O_RDWR, 0660)
	try:
		# source /etc/profile to include hadoop home
		SplitLog.lock(fd)
		spliter = SplitLog()
		if mode == 'day':
			if not start:
				now = time_start(datetime.datetime.now(),'day')
				start = now - datetime.timedelta(days=1)
			end = start + datetime.timedelta(days=1)
		if start and end:
			while start < end:
				spliter.process(jobtype,start)
				start += datetime.timedelta(hours=1)
		else:
			spliter.process(jobtype)
	except Exception,e:
		logging.error('%s\n',str(e),exc_info=True)
	finally:
		os.close(fd)
		os.remove(lock_file_path)

