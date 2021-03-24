#!/bin/bash
 
# 定义变量方便修改
dwd_dbname=hdp_zhuanzhuan_dwd_global
dws_dbname=hdp_zhuanzhuan_dws_global
hive=/work/hadoop/hive-2.3.4/bin/hive
 
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n $1 ] ;then
	log_date=$1
else 
	log_date=`date  -d "-1 day"  +%F`  
fi 
 
sql="
 
with  
tmp_order as
(
    select 
      user_id, 
      sum(oc.total_amount) order_amount, 
      count(*)  order_count
    from "$dwd_dbname".dwd_mysql_order_info_inc_1d  oc
    where date_format(oc.create_time,'yyyy-MM-dd')='$log_date'
    group by user_id
)  ,
tmp_payment as
(
    select 
      user_id, 
      sum(pi.total_amount) payment_amount, 
      count(*) payment_count 
    from "$dwd_dbname".dwd_mysql_payment_info_inc_1d pi 
    where date_format(pi.payment_time,'yyyy-MM-dd')='$log_date'
    group by user_id
),
tmp_comment as
(  
    select  
      user_id, 
      count(*) comment_count
    from "$dwd_dbname".dwd_log_base_event_inc_1d c
    where date_format(c.dt,'yyyy-MM-dd')='$log_date' and c.event_name='comment'
    group by user_id 
)
 
insert  overwrite table "$dws_dbname".dws_traffic_ub_inc_1d partition(dt='$log_date')
select 
    user_actions.user_id, 
    sum(user_actions.order_count), 
    sum(user_actions.order_amount),
    sum(user_actions.payment_count), 
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count) 
from 
(
    select 
      user_id, 
      order_count,
      order_amount ,
      0 payment_count , 
      0 payment_amount, 
      0 comment_count 
    from tmp_order 
 
    union all
    select 
      user_id, 
      0,
      0, 
      payment_count, 
      payment_amount,
      0  
    from tmp_payment
 
    union all
    select 
      user_id, 
      0,
      0,
      0,
      0,
      comment_count 
    from tmp_comment
 ) user_actions
group by user_id;
"

$hive -e "$sql"
