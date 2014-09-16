#!/bin/sh
source ~/.bashrc
date=`date +%Y-%m-%d`
yesterday=`date +%Y-%m-%d -d -1day`

python /home/wangchunyang/check.py $yesterday

if [ $? -eq 1 ]
then
    echo "no data found dm_v6[${yesterday}], exit!" > /home/wangchunyang/logs/error.${date}
    exit 1
fi
cd /home/wangchunyang/code/logstat/analyze/
python stat2db.py --mode=day > /home/wangchunyang/logs/stat2db.log.${date} 2>&1

year=${yesterday:0:4}
month_bef=${yesterday:5:2}
month=""
case $month_bef in
"01") month="1";;
"02") month="2";;
"03") month="3";;
"04") month="4";;
"05") month="5";;
"06") month="6";;
"07") month="7";;
"08") month="8";;
"09") month="9";;
*) month=$month_bef
esac

table=basic_stat_${year}_${month}
echo $table
mysql -uanaly -panaly123 -h 192.168.0.227 -P 3306 << EOF
use logstat_new;
update $table b,(SELECT sum(new_user_run) as num FROM $table where scope_id in ('39', '40', '41', '42', '151988','321972',' 443939') and time = DATE_FORMAT(DATE_SUB(current_date, INTERVAL 1 DAY), '%Y-%m-%d')) c set b.new_user_run = c.num where b.scope_id = 1 and b.time = DATE_FORMAT(DATE_SUB(current_date, INTERVAL 1 DAY), '%Y-%m-%d');
EOF

cd $HOME/logs/
find ./ -mtime +2 | xargs rm -rf
