package tm

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.SparkSession


/**
  * 订单商家用户相关信息汇总 2018/12/02
  *
  * @author zhanghao
  */
object tm_order_detail_d {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("tm_order_detail")
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


    import spark.sql

    //订单信息表
    val order = sql(
      s"""
         |SELECT
         |order_id,
         |province_id,
         |province_name,
         |city_id,
         |city_name,
         |poi_id,
         |user_id,
         |platform_id,
         |platform_name,
         |terminal_id,
         |terminal_name,
         |fee,
         |order_time,
         |remark,
         |shipping_fee,
         |rider,
         |delivery_type_id,
         |delivery_name,
         |delivery_time,
         |status,
         |day_id
         |FROM waimai.tf_order_info_d
         |WHERE day_id='$day_id'
      """.stripMargin)

    //商家信息表
    val poi = sql(
      s"""
         |SELECT
         |poi_id,
         |poi_name,
         |phone,
         |address,
         |day_id
         |FROM waimai.tf_poi_info_d
         |WHERE day_id='$day_id'
      """.stripMargin)

    //将订单信息表和商家订单信息表关联到一起，生成临时表
    val tmpTable1 = order.join(poi, Seq("poi_id", "day_id"), "left_outer")
    tmpTable1.show(5)
    //将临时表与用户表进行关联
    val user = sql(
      s"""
         |SELECT user_id,
         |user_name,
         |gender,
         |day_id
         |FROM waimai.tf_user_info_d
         |WHERE day_id='$day_id'
      """.stripMargin)
    val tmpTable2 = tmpTable1.join(user, Seq("user_id", "day_id"), "left_outer")
    tmpTable2.show(5)
    //将活动表根据订单维度打成平台补贴和商家补贴
    val act = sql(
      s"""
         |SELECT order_id,
         |day_id,
         |sum(case when act_type_id=1 then charge_fee else 0 end) as plat_charge_fee,
         |sum(case when act_type_id=2 then charge_fee else 0 end) as poi_charge_fee
         |FROM waimai.tf_order_act_d
         |WHERE day_id='$day_id'
         |GROUP BY order_id,day_id
      """.stripMargin)

    act.show(5)
    val tmpTable3 = tmpTable2.join(act, Seq("order_id", "day_id"), "left_outer")
    tmpTable3.show(5)
    tmpTable3.createOrReplaceTempView("tmpTable3")

    //关联用户首购表，打出新老用户标识

    val tmpTable4 = sql(
      s"""
         |SELECT
         |t.order_id,
         |t.province_id,
         |t.province_name,
         |t.city_id,
         |t.city_name,
         |t.poi_id,
         |t.poi_name,
         |t.phone,
         |t.address,
         |t.user_id,
         |t.user_name,
         |t.gender,
         |if(first.user_id is not null,1,0) as new_old_id,
         |t.platform_id,
         |t.platform_name,
         |t.terminal_id,
         |t.terminal_name,
         |t.fee,
         |t.order_time,
         |t.remark,
         |t.shipping_fee,
         |t.rider,
         |t.delivery_type_id,
         |t.delivery_name,
         |t.delivery_time,
         |t.status,
         |t.plat_charge_fee,
         |t.poi_charge_fee,
         |t.day_id
         |FROM tmpTable3 t
         |LEFT OUTER JOIN waimai.tf_user_first_pur_d first
         |ON t.day_id=first.day_id
         |AND t.user_id=first.user_id
         |WHERE t.day_id='$day_id'
      """.stripMargin)
    tmpTable4.show(5)
    tmpTable4.createOrReplaceTempView("tmpTable4")

    //设置hive的相关参数
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions", "10000")
    spark.sqlContext.setConf("hvie.exec.max.dynamic.partitions.pernoden", "1000")


    val createTable =
      """CREATE TABLE IF NOT EXISTS waimai.tm_order_detail_d(
        |order_id int ,
        |province_id int,
        |province_name string,
        |city_id int,
        |city_name string,
        |poi_id int,
        |poi_name int,
        |phone string,
        |address string,
        |user_id int,
        |user_name string,
        |gender string,
        |new_old_id int,
        |platform_id int,
        |platform_name string,
        |terminal_id int,
        |terminal_name string,
        |fee double,
        |order_time string,
        |remark string ,
        |shipping_fee double,
        |rider string,
        |delivery_type_id int,
        |delivery_name string ,
        |delivery_time string,
        |status int,
        |plat_charge_fee double,
        |poi_charge_fee double)
        |COMMENT '订单商家用户信息汇总'
        |PARTITIONED BY(day_id string)
        |STORED AS orc
      """.stripMargin

    println(createTable)
    sql(createTable)

    //保存数据到目标表中
    val insertSql =
      """
        |INSERT OVERWRITE TABLE waimai.tm_order_detail_d partition(day_id)
        |SELECT
        |t.order_id,
        |t.province_id,
        |t.province_name,
        |t.city_id,
        |t.city_name,
        |t.poi_id,
        |t.poi_name,
        |t.phone,
        |t.address,
        |t.user_id,
        |t.user_name,
        |t.gender,
        |coalesce(info.new_old_name,'未知') as new_old_name,
        |t.platform_id,
        |t.platform_name,
        |t.terminal_id,
        |t.terminal_name,
        |t.fee,
        |t.order_time,
        |t.remark,
        |t.shipping_fee,
        |t.rider,
        |t.delivery_type_id,
        |t.delivery_name,
        |t.delivery_time,
        |t.status,
        |t.plat_charge_fee,
        |t.poi_charge_fee,
        |t.day_id
        |FROM tmpTable4 t
        |LEFT OUTER JOIN waimai.td_new_old_info info
        |ON t.new_old_id=info.new_old_id
      """.stripMargin
    sql(insertSql)
    spark.stop()


  }
}
