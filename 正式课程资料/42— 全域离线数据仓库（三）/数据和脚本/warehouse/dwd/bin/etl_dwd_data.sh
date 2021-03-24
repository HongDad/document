#!/bin/bash

# 定义变量方便修改
ods_dbname=hdp_zhuanzhuan_ods_global
dwd_dbname=hdp_zhuanzhuan_dwd_global
hive=/work/hadoop/hive-2.3.4/bin/hive
 
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n $1 ] ;then
	log_date=$1
else 
	log_date=`date  -d "-1 day"  +%F`  
fi 
 
sql="
 
set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table   "$dwd_dbname".dwd_mysql_order_info_inc_1d partition(dt)
select  * from "$ods_dbname".ods_zz_order_info_inc_1d 
where dt='$log_date'  and id is not null;
 
insert  overwrite table   "$dwd_dbname".dwd_mysql_order_detail_inc_1d partition(dt)
select  * from "$ods_dbname".ods_zz_order_detail_inc_1d 
where dt='$log_date' and id is not null;

insert  overwrite table   "$dwd_dbname".dwd_mysql_payment_info_inc_1d partition(dt)
select  * from "$ods_dbname".ods_zz_payment_info_inc_1d 
where dt='$log_date' and id is not null;
 
insert  overwrite table   "$dwd_dbname".dwd_mysql_user_info_full_1d partition(dt)
select  * from "$ods_dbname".ods_zz_user_info_full_1d 
where dt='$log_date'   and id is not null;
 
insert  overwrite table   "$dwd_dbname".dwd_mysql_sku_info_full_1d partition(dt)
select  
    sku.id,
    sku.spu_id, 
    sku.price,
    sku.sku_name,  
    sku.sku_desc,  
    sku.weight,  
    sku.tm_id,  
    sku.category3_id,  
    c2.id category2_id ,  
    c1.id category1_id,  
    c3.name category3_name,  
    c2.name category2_name,  
    c1.name category1_name,  
    sku.create_time,
    sku.dt
from
    "$ods_dbname".ods_zz_sku_info_full_1d sku 
join "$ods_dbname".ods_zz_category3_full_1d c3 on sku.category3_id=c3.id 
    join "$ods_dbname".ods_zz_category2_full_1d c2 on c3.category2_id=c2.id 
    join "$ods_dbname".ods_zz_category1_full_1d c1 on c2.category1_id=c1.id 
where sku.dt='$log_date'  and c2.dt='$log_date'  
and  c3.dt='$log_date' and  c1.dt='$log_date' 
and sku.id is not null;

insert overwrite table "$dwd_dbname".dwd_log_base_start_inc_1d 
PARTITION (dt)
select
mid_id,
user_id,
version_code,
version_name,
lang,
source ,
os ,
area ,
model ,
brand ,
sdk_version ,
gmail ,
height_width ,
app_time ,
network ,
lng ,
lat ,
event_name , 
event_json , 
server_time , 
dt 
from "$ods_dbname".ods_log_start_app_inc_1d lateral view json_tuple(line,'mid','uid','vc','vn','l','sr','os','ar','md','ba','sv','g','hw','t','nw','ln','la','en','kv','ett') tmp_k as mid_id,user_id,version_code,version_name,lang,source,os,area,model,brand,sdk_version,gmail,height_width,app_time,network,lng,lat,event_name, event_json,server_time where dt='$log_date' and event_name<>'' and server_time>0 and mid_id<>'';

insert overwrite table "$dwd_dbname".dwd_log_base_event_inc_1d 
PARTITION (dt)
select
mid_id,
user_id,
version_code,
version_name,
lang,
source ,
os ,
area ,
model ,
brand ,
sdk_version ,
gmail ,
height_width ,
app_time ,
network ,
lng ,
lat ,
event_name , 
event_json , 
server_time , 
dt 
from "$ods_dbname".ods_log_event_inc_1d lateral view json_tuple(line,'mid','uid','vc','vn','l','sr','os','ar','md','ba','sv','g','hw','t','nw','ln','la','en','kv','ett') tmp_k as mid_id,user_id,version_code,version_name,lang,source,os,area,model,brand,sdk_version,gmail,height_width,app_time,network,lng,lat,event_name, event_json,server_time where dt='$log_date' and event_name<>'' and server_time>0 and mid_id<>'';

"
$hive -e "$sql"
