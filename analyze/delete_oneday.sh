#!/bin/sh
yesterday=""
today=""
if [ $# -eq 2 ]
then
yesterday=$1
today=$2
else
yesterday=`date +%Y-%m-%d -d -1day`
today=`date +%Y-%m-%d`
fi
year=${yesterday:0:4}
month_bef=${yesterday:5:2}
#printf "%d" $day
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
echo "delete from book_stat_${year}_${month} where time >= '${yesterday}' and time < '${today}';"

#today=`date +%Y-%m-%d`
#yesterday=`date +%Y-%m-%d -d -1day`
mysql -uanaly -panaly123 -h 192.168.0.227 -P 3306 << EOF

use logstatV2;
delete from book_stat_${year}_${month} where time >= '${yesterday}'and time < '${today}';
#delete from basic_stat_${year}_${month} where time >= '${yesterday}'and time < '${today}';
delete from book_refer_stat_${year}_${month} where time >= '${yesterday}'and time < '${today}';
#delete from book_tag_stat_${year}_${month} where time >= '${yesterday}'and time < '${today}';
#delete from topn_stat where time >= '${yesterday}'and time < '${today}';
#delete from visit_stat where time >= '${yesterday}'and time < '${today}';
#delete from product_stat where time >= '${yesterday}'and time < '${today}';
delete from factory_sum_stat where time >= '${yesterday}'and time < '${today}';

use logstatV3;
delete from basic_stat_${year}_${month} where time >= '${yesterday}'and time < '${today}';

EOF





