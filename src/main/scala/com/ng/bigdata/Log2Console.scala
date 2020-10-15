package com.ng.bigdata

import com.ng.bigdata.conf.Config
import com.ng.bigdata.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @User: kaisy
 * @Date: 2020/9/28 18:43
 * @Desc: 消费Kafka数据，输出控制台
 */
object Log2Console {
  def main(args: Array[String]): Unit = {
    // todo 设置logger
    Logger.getLogger("org").setLevel(Level.WARN)
    // todo 加载解析命令
    val config: Config = Config.parseConfig(Log2Console, args)
    // todo 初始化上下文
    val spark: SparkSession = SparkHelper.getSparkSession(config.env)
    // todo 导入隐式转换
    import spark.implicits._

    // todo 获取kafka的数据
    val df: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", config.brokerList)
      .option("subscribe", config.sourceTopic)
      .option("startingOffsets", "earliest")
      .option("kafka.consumer.commit.groupid", "test")
      .load()

    // todo 打印数据
    df.selectExpr("cast(value as String)","offset")
      .as[(String,Long)]
      .writeStream
      .queryName("action-log")
      .outputMode(OutputMode.Append)
      .format("console")
      .start()
      .awaitTermination()

  }
}
