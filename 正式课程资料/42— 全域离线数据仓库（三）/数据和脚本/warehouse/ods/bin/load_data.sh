#!/bin/bash

do_date=$1
dbname=hdp_zhuanzhuan_ods_global
hive=/work/hadoop/hive-2.3.4/bin/hive
 
sql=" 
load data local inpath '/home/work/origin_data/db/zz_order_info/$do_date'  OVERWRITE into table $dbname"".ods_zz_order_info_inc_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_order_detail/$do_date'  OVERWRITE into table $dbname"".ods_zz_order_detail_inc_1d partition(dt='$do_date');

load data local inpath '/home/work/origin_data/db/zz_payment_info/$do_date'  OVERWRITE into table $dbname"".ods_zz_payment_info_inc_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_sku_info/$do_date'  OVERWRITE into table $dbname"".ods_zz_sku_info_full_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_user_info/$do_date' OVERWRITE into table $dbname"".ods_zz_user_info_full_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_category1/$do_date' OVERWRITE into table $dbname"".ods_zz_category1_full_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_category2/$do_date' OVERWRITE into table $dbname"".ods_zz_category2_full_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/db/zz_category3/$do_date' OVERWRITE into table $dbname"".ods_zz_category3_full_1d partition(dt='$do_date'); 

load data local inpath '/home/work/origin_data/log/start_app/$do_date' OVERWRITE into table $dbname"".ods_log_start_app_inc_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/log/event/$do_date' OVERWRITE into table $dbname"".ods_log_event_inc_1d partition(dt='$do_date');
"
$hive -e "$sql"
