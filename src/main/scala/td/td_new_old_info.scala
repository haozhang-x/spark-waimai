package td

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * 新老用户标识维表
  * 2018/12/01
  *
  * @author zhanghao
  *
  */
object td_new_old_info {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("td_new_old_info")
      .master("local[*]")
      .getOrCreate()

    val newOldDF = spark.read.
      format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/waimai")
      .option("dbtable", "td_new_old_info")
      .option("user", "root")
      .option("password", "abc123456")
      .load()

    newOldDF.write.mode(SaveMode.Append).format("orc").saveAsTable("waimai.td_new_old_info")


  }

}
