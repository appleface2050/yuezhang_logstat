#! SHELL=/bin/sh
# logstat.zhangyue.com deploy 

project_path=./
GIT=/usr/local/bin/git
CTL=/usr/local/bin/supervisorctl

start:
	${CTL} start 'zhangyue:zhangyue-logstat-896'{0..3}

stop:
	${CTL} stop 'zhangyue:zhangyue-logstat-896'{0..3}

env:
	${GIT} pull

logstat:
	${CTL} restart 'zhangyue:zhangyue-logstat-896'{0..3}

update:
	make env
	make logstat

