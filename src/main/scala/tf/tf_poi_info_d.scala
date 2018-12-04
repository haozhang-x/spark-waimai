package tf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession

/**
  * 商家信息
  * 2018/12/02
  *
  * @author zhanghao
  */
object tf_poi_info_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("tf_poi_info_d")
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


    val poi = spark.sql(
      s"""
         |SELECT poi_id,
         |poi_name,
         |city_id,
         |phone,
         |address,
         |day_id
         |FROM waimai.ti_poi_info_d
         |WHERE day_id='$day_id'
      """.stripMargin)

    val city = spark.sql(
      """
        |SELECT province_id,
        |province_name,
        |city_id,
        |city_name
        |FROM waimai.td_city_info
      """.stripMargin)

    val poiDF = poi.join(city, Seq("city_id"), "left_outer")
    poiDF.show(5)

    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions.pernoden", "1000")

    //spark.sql("DROP TABLE IF EXISTS waimai.tf_poi_info_d")

    val createTable =
      """
        |CREATE TABLE IF NOT EXISTS waimai.tf_poi_info_d(
        |poi_id int,
        |poi_name string,
        |province_id int,
        |province_name string,
        |city_id int,
        |city_name string,
        |phone string,
        |address string)
        |COMMENT '商家信息表'
        |PARTITIONED BY (day_id string)
        |STORED AS orc
      """.stripMargin

    println(createTable)
    spark.sql(createTable)
    poiDF.createOrReplaceTempView("poi")
    //保存数据到目标表中
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE waimai.tf_poi_info_d PARTITION(day_id)
         |SELECT poi_id,
         |poi_name,
         |province_id,
         |province_name,
         |city_id,
         |city_name,
         |phone,
         |address,
         |day_id
         |FROM poi
         |WHERE day_id='$day_id'
      """.stripMargin)

    spark.stop()

  }
}
