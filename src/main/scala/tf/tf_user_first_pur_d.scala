package tf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 用户首购表 2018/12/02
  *
  * @author zhanghao
  */
object tf_user_first_pur_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("tf_user_first_pur_d")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //处理day_id
    var day_id = ""
    //获取昨天的日期
    val yesterday = LocalDate.now.minus(1, ChronoUnit.DAYS) format DateTimeFormatter.ofPattern("yyyyMMdd")
    //如果没有传参，则默认昨天
    if (args.length < 1) {
      day_id = yesterday
    } else {
      day_id = args(0)
    }

    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.tf_user_first_pur_d(
        |order_id int,
        |user_id int,
        |poi_id int)
        |COMMENT '用户首次购买记录表'
        |PARTITIONED BY (day_id string)
        |STORED AS orc
      """.stripMargin
    println(createTable)
    spark.sql(createTable)


    //使用窗口函数将每天发生的第一单的用户拿出来，然后和历史的首购用户进行比较，历史没有出现过的用户才是新用户
    val sql =
      s"""
         |SELECT a.order_id,a.user_id,a.poi_id,a.day_id
         |FROM(
         |SELECT order_id,user_id,poi_id,day_id,row_number() over(distribute by user_id sort by order_time asc) rn
         |FROM waimai.ti_order_delta_d
         |WHERE day_id='$day_id') a
         |LEFT OUTER JOIN (
         |SELECT user_id FROM waimai.tf_user_first_pur_d WHERE day_id <'$day_id'
         |) b
         |ON a.user_id=b.user_id
         |WHERE a.user_id=b.user_id
         |AND a.rn=1
      """.stripMargin
    println(sql)
    val userFirstDF = spark.sql(sql)
    userFirstDF.show(5)

    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions.pernoden", "1000")

    userFirstDF.createOrReplaceTempView("user")

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE waimai.tf_user_first_pur_d PARTITION(day_id)
         |SELECT order_id,
         |user_id,
         |poi_id,
         |day_id
         |FROM user
         |WHERE day_id='$day_id'
      """.stripMargin)

    spark.stop()

  }
}
