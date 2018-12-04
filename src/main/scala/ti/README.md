#接口层

先source wai.sql到waimai数据库中,可用如下的命令
```
mysql -u root -p waimai -e "source waimai.sql"
```

修改ti_sqoop_import.sh中的信息
导入到Hive中

```
bash ti_sqoop_import.sh
```
