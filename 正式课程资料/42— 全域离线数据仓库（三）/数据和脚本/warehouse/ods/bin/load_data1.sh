#!/bin/bash

do_date=$1
dbname=hdp_zhuanzhuan_ods_global
hive=/work/hadoop/hive-2.3.4/bin/hive
 
sql=" 

load data local inpath '/home/work/origin_data/log/start_app/$do_date' OVERWRITE into table $dbname"".ods_log_start_app_inc_1d partition(dt='$do_date');
 
load data local inpath '/home/work/origin_data/log/event/$do_date' OVERWRITE into table $dbname"".ods_log_event_inc_1d partition(dt='$do_date');
"
$hive -e "$sql"
