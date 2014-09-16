#!/bin/sh 
source /etc/profile
pig=/home/hadoop/pig/bin/pig
mysql=/usr/local/mysql-5.1.63/bin/mysql
yesterday=`date +%Y-%m-%d -d -1day`

$mysql -uanaly -panaly123 -h 192.168.0.227 -P 3306 << EOF
use logstatV2;
truncate table push_click;
EOF

$pig -p fn=/user/hive/warehouse/SERVICES_LOG_V6/ds=$yesterday -p date=$yesterday pushobtain.pig
$pig -p fn=/user/hive/warehouse/SERVICES_LOG_V6/ds=$yesterday -p date=$yesterday pushclick.pig
