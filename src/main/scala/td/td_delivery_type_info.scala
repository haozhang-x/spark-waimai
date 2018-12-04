package td

import org.apache.spark.sql.SparkSession

/**
  *
  * 配送维度信息表
  * 2018/12/01
  *
  * @author zhanghao
  */
object td_delivery_type_info {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("td_delivery_info")
      .master("local[*]")
      .getOrCreate()

    val delivery = spark.read.json("hdfs://localhost:9000/waimai/data/delivery.json")
    delivery.show()
    delivery.write.format("orc").saveAsTable("waimai.td_delivery_type_info")
    spark.stop()


  }
}
