package ta

import org.apache.spark.sql.SparkSession

/**
  * 参数总体概览（周）
  * 2018/12/02
  *
  * @author zhanghao
  */
object ta_total_overview_w_view {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ta_total_overview_d_view")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("DROP VIEW IF EXISTS waimai.ta_total_overview_w_view")

    sql(
      """
        |CREATE VIEW IF NOT EXISTS waimai.ta_total_overview_w_view
        |AS
        |SELECT
        |b.week_id ,
        |a.province_id,
        |a.province_name,
        |a.city_id,
        |a.city_name,
        |a.platform_id,
        |a.platform_name,
        |a.terminal_id,
        |a.terminal_name,
        |a.order_id as order_num,
        |a.user_id as user_num,
        |a.plat_charge_fee+a.poi_charge_fee as charge_fee,
        |a.fee as original_fee,
        |a.fee-shipping_fee-a.plat_charge_fee-a.poi_charge_fee as actual_fee
        |FROM waimai.tm_order_detail_d a
        |LEFT OUTER JOIN waimai.td_date_info b
        |ON a.day_id=b.day_id
      """.stripMargin)

    spark.stop()

  }
}
