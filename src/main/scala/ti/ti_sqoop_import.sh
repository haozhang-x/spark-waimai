#!/usr/bin/env bash
#sqoop 导入外卖数据库里面里面的除td_new_old_info以外的表
<<'Tables_in_waimai'
+---------------------------+
| td_new_old_info           |
| ti_order_delivery_delta_d |
| ti_order_delta_d          |
| ti_order_plat_act_d       |
| ti_order_poi_act_d        |
| ti_poi_info_d             |
| ti_user_info_d            |
+---------------------------+
'Tables_in_waimai'
#sqoop import
sqoop import-all-tables   \
--connect "jdbc:mysql://localhost:3306/waimai?zeroDateTimeBehavior=CONVERT_TO_NULL" \
--username "root" \
--password "abc123456" \
--create-hive-table \
--hive-database "waimai" \
--hive-import \
--fields-terminated-by "," \
--num-mappers 1 \
--exclude-tables  "td_new_old_info" \
--outdir "/tmp/sqoop/"