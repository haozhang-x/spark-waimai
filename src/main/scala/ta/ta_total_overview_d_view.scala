package ta

import org.apache.spark.sql.SparkSession

/**
  * 参数总体概览（日）
  * 2018/12/02
  *
  * @author zhanghao
  */
object ta_total_overview_d_view {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ta_total_overview_d_view")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("DROP VIEW IF EXISTS waimai.ta_total_overview_d_view")

    sql(
      """
        |CREATE VIEW IF NOT EXISTS waimai.ta_total_overview_d_view
        |AS
        |SELECT
        |day_id,
        |province_id,
        |province_name,
        |city_id,
        |city_name,
        |platform_id,
        |platform_name,
        |terminal_id,
        |terminal_name,
        |order_id as order_num,
        |user_id as user_num,
        |plat_charge_fee+poi_charge_fee as charge_fee,
        |fee as original_fee,
        |fee-shipping_fee-plat_charge_fee-poi_charge_fee as actual_fee
        |FROM waimai.tm_order_detail_d
      """.stripMargin)
    spark.stop()

  }
}
