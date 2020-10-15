package com.ng.bigdata

import com.ng.bigdata.conf.Config
import com.ng.bigdata.extract.NewsAction
import com.ng.bigdata.util.{HudiConfig, Meta, SparkHelper}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @User: kaisy
 * @Date: 2020/9/30 9:04
 * @Desc: 将数据存入Hudi，形成User表和Event表
 */
object Log2Hudi {
  def main(args: Array[String]): Unit = {
    // tip 设置Log
    Logger.getLogger("Log2HDFS").setLevel(Level.WARN)
    // tip 设置系统参数
    System.setProperty("HADOOP_USER_NAME","root")
    // tip 解析命令行参数
    val config: Config = Config.parseConfig(Log2Hudi, args)

    // todo 实例化上下文
    val spark: SparkSession = SparkHelper.getSparkSession(config.env)
    // todo 导入隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // todo 获取kafka数据
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.brokerList)
      .option("subscribe", config.sourceTopic)
      .option("startingOffsets", "earliest")
      .option("kafka.consumer.commit.groupid", "test")
      .load()

    // todo 为了方便从Meta表中读取元数据字段，加载Schema，并转成DS
    val eventMeta: DataFrame = spark.read.json(Seq(Meta.getMetaJson("event")).toDS)
    val userMeta: DataFrame = spark.read.json(Seq(Meta.getMetaJson("user")).toDS)

    // todo 将元数据广播出去，使用流来处理数据
    val msb: Broadcast[(StructType, StructType)] = spark.sparkContext.broadcast((eventMeta.schema, userMeta.schema))

    // todo 读取数据
    val ds: Dataset[String] = df.selectExpr("cast(value as String)").as[String]
    // todo 将数据解析，并按照Type类型拆分数据，写入Hudi
    ds.writeStream
      .queryName("action2hudi")
      .option("checkpointLocation",config.checkpointDir + "/action/")
      .trigger(Trigger.ProcessingTime(config.trigger + " seconds"))
      .foreachBatch((batchDF: Dataset[String], batchId: Long) => {
        // tip 开启缓存，可复用
        batchDF.persist()
        // tip 解析数据
        val newDS: Dataset[String] = batchDF.map(NewsAction.apply.unionMetaAndBody)
          .filter(_ != null)
        // tip 动态生成schema，创建Event表和User表元数据
        if(!newDS.isEmpty){
          // tip 转换格式 存储Event表
          newDS.select(from_json($"value",msb.value._1).as("data_event"))
            .select("data_event.*") // 查询所有列
            .filter("type = 'track'") // 过滤出event数据
            .write
            // tip 写入Hudi表，并加载Hudi配置
            .format("org.apache.hudi")
            .options(HudiConfig.getEventConfig(
              config.tableType,config.syncJDBCUrl,config.syncJDBCUsername
            ))
            // tip 加载hudu的数据
            .option(HoodieWriteConfig.TABLE_NAME,"event")
            // tip 将数据映射至hive表，类似于HDFS数据存储到HIVE表下面，必须要load一样
            .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY,config.syncDB)
            // tip Hudi存储是分区的话，映射hive要开启分区更新操作
            .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH,"true")
            // tip 四种索引模式，主要是为了我Hive检所查询的数据方式
            .option(HoodieIndexConfig.INDEX_TYPE_PROP,HoodieIndex.IndexType.GLOBAL_BLOOM.name())
            .mode(SaveMode.Append) // 存储模式
            .save(config.hudiEventBasePath) // 存储路径
          // tip 转换格式，存储user表
          newDS.select(from_json($"value",msb.value._2).as("data_user"))
            .select("data_user.*") // 查询所有列
            .filter("type = 'profile_set'") // 过滤出event数据
            .write
            // tip 写入Hudi表
            .format("org.apache.hudi")
            .options(HudiConfig.getEventConfig(
              config.tableType,config.syncJDBCUrl,config.syncJDBCUsername
            ))
            // tip 加载hudu的数据
            .option(HoodieWriteConfig.TABLE_NAME,"user")
            // tip 将数据映射至hive表，类似于HDFS数据存储到HIVE表下面，必须要load一样
            .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY,config.syncDB)
            // tip Hudi存储是分区的话，映射hive要开启分区更新操作
            .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH,"true")
            // tip 四种索引模式，主要是为了我Hive检所查询的数据方式
            .option(HoodieIndexConfig.INDEX_TYPE_PROP,HoodieIndex.IndexType.GLOBAL_BLOOM.name())
            .mode(SaveMode.Append) // 存储模式
            .save(config.hudiUserBasePath) // 存储路径
        }
        // 释放缓存
        batchDF.unpersist()
      })
      .start()
      .awaitTermination()
  }
}
