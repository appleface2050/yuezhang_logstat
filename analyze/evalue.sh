#!/bin/sh
start=$1
end=$2
other=$3
hive <<EOF
use ana_tempdb;
drop table ana_tempdb.temp_downv6_chptbook_evalu_v2;
create table ana_tempdb.temp_downv6_chptbook_evalu_v2 AS
select count(distinct a.user_name)usnum,sum(a.price)cfee,count(distinct (case when a.price>0 then a.user_name else '' end)) pay_usnum,a.bid,b.startpoint from (select user_name,bid,cid,min(price) price,ds from default.download_v6_simp where ds>=$start and ds<$end and down_type=2 and fee_Unit=20 and price_code!=9 group by user_name,bid,cid,ds)a join (select * from ana_tempdb.temp_downv6_1u1b_chpt_stpoint_v1 where firstds<$other)b on a.user_name=b.user_name and a.bid=b.bid group by a.bid,b.startpoint;
drop table if exists ana_tempdb.temp_downv6_1u1b_cfree_stpoint ;
create table ana_tempdb.temp_downv6_1u1b_cfree_stpoint AS
select user_name,bid,min(ds) firstds,max(ds) lastds,case when min(cast(cid as bigint)) in(1,2,3) then 1 when min(cast(cid as bigint)) in(21,22,23) then 21 else 3 end startpoint from default.download_v6_simp where ds>=$start and ds<$end and down_type=2 and fee_unit=20 and price=0 and cid!='' group by user_name,bid;
drop table if exists ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon;
create table ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon AS
select a.* from ana_tempdb.temp_downv6_1u1b_cfree_stpoint a join (select distinct user_name,bid from default.download_v6_simp where ds>=$start and ds <$end and down_type=2 and fee_unit=20) b on a.user_name=b.user_name and a.bid=b.bid where a.firstds<$other ;
drop table if exists ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1 ;
create table ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1 AS
select user_name,bid,min(ds) firstds,max(ds) lastds, min(cast(cid as bigint)) startpoint from download_v6_simp where ds>=20121001 and ds<$end and down_type=2 and fee_unit=20 and price=0 and cid!='' group by user_name,bid;
drop table if exists ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon_v1;
create table ana_tempdb.temp_downv6_1u1b_cfree_stpoint_3mon_v1 AS 
select a.* from ana_tempdb.temp_downv6_1u1b_cfree_stpoint_v1 a join (select distinct user_name,bid from default.download_v6_simp where ds>=20130101 and ds <$end and down_type=2 and fee_unit=20) b on a.user_name=b.user_name and a.bid=b.bid where a.firstds<$other;
exit;
EOF
