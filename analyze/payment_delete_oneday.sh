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
echo ${today}
#today=`date +%Y-%m-%d`
#yesterday=`date +%Y-%m-%d -d -1day`
mysql -uanaly -panaly123 -h 192.168.0.227 -P 3306 << EOF

use payment_v6;
delete from payment_pay_daily_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_recharging_amount_finish_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_recharging_amount_order_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_recharging_detail_finish_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_recharging_detail_order_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_recharging_detail_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_banlance_stat where time >= '${yesterday}'and time < '${today}';
delete from payment_user_run where time >= '${yesterday}'and time < '${today}';

EOF





