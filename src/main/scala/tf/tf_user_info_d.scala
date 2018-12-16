package tf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 用户信息 2018/12/02
  *
  * @author zhanghao
  */
object tf_user_info_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("tf_user_info_d")
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


    val user = spark.sql(
      s"""
         |SELECT user_id,
         |user_name,
         |gender,
         |regist_time,
         |day_id
         |FROM waimai.ti_user_info_d
         |WHERE day_id='$day_id'
      """.stripMargin)


    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hive.exec.max.dynamic.partitions.pernoden", "1000")

    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.tf_user_info_d(
        |user_id int,
        |user_name string,
        |gender string,
        |regist_time string
        |)COMMENT '用户信息表'
        |PARTITIONED BY (day_id string)
        |STORED AS orc
      """.stripMargin
    println(createTable)

    spark.sql(createTable)
    user.createOrReplaceTempView("user")
    //保存数据到目标表
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE  waimai.tf_user_info_d partition(day_id)
         |SELECT user_id,
         |user_name,
         |gender,
         |regist_time,
         |day_id
         |FROM user
         |WHERE day_id='$day_id'
      """.stripMargin)

    spark.stop()
  }
}
