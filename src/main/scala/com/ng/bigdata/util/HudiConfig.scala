package com.ng.bigdata.util

import scala.collection.mutable

/**
  * @description:
  * @author: QF
  * @date: 2020/6/13 11:15 AM
  * @version V1.0
  */


object HudiConfig {

  def getEventConfig(tableType:String,hiveJDBCURL:String,hiveJDBCUsername:String): mutable.HashMap[String, String] = {


    val props = new mutable.HashMap[String, String]
    tableType.toUpperCase() match {
      case "COW" =>
        props.put("hoodie.datasource.write.table.type","COPY_ON_WRITE")
      case "MOR" =>
        props.put("hoodie.datasource.write.table.type","MERGE_ON_READ")
        //是否启用压缩
        props.put("hoodie.compact.inline", "true")
        // 多少个delta的comtis之后触发压缩
        props.put("hoodie.compact.inline.max.delta.commits", "5")
      case _ =>
        props.put("hoodie.datasource.write.table.type","COPY_ON_WRITE")
    }

    // 写入模式
    props.put("hoodie.datasource.write.operation", "insert")
    // 设置主键列名
    props.put("hoodie.datasource.write.recordkey.field", "uuid")
    // 设置数据更新时的列名
    props.put("hoodie.datasource.write.precombine.field", "uuid")
    // 分区列设置
    props.put("hoodie.datasource.write.partitionpath.field", "logday")

    props.put("hoodie.commits.archival.batch","2")
    props.put("hoodie.cleaner.commits.retained","2")
    props.put("hoodie.keep.min.commits","3")
    props.put("hoodie.keep.max.commits","4")

    // insert upsert 初始并行度
    props.put("hoodie.insert.shuffle.parallelism", "1")
    props.put("hoodie.upsert.shuffle.parallelism", "1")

    // 设置要同步的hive库名
    props.put("hoodie.datasource.hive_sync.database", "default")
    // 设置要同步的hive表名
    props.put("hoodie.datasource.hive_sync.table", "event")
    // 设置数据集注册或同步到hive metastore
    props.put("hoodie.datasource.hive_sync.enable", "true")
    // 设置要同步的分区列名
    props.put("hoodie.datasource.hive_sync.partition_fields", "logday")
    // 设置jdbc 连接同步
    //jdbc:hive2://172.17.106.165:10000
    props.put("hoodie.datasource.hive_sync.jdbcurl", hiveJDBCURL)
    // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
    props.put("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
    // hive username
    props.put("hoodie.datasource.hive_sync.username", hiveJDBCUsername)

    props

  }

  def getUserConfig(tableType:String,hiveJDBCURL:String,hiveJDBCUsername:String): mutable.HashMap[String, String] = {

    val props = new mutable.HashMap[String, String]
    tableType.toUpperCase match {
      case "COW" =>
        props.put("hoodie.datasource.write.table.type","COPY_ON_WRITE")
      case "MOR" =>
        props.put("hoodie.datasource.write.table.type","MERGE_ON_READ")
        //是否启用压缩
        props.put("hoodie.compact.inline", "true")
        // 多少个delta的comtis之后触发压缩
        props.put("hoodie.compact.inline.max.delta.commits", "5")
      case _ =>
        props.put("hoodie.datasource.write.table.type","COPY_ON_WRITE")
    }
    // user表设定为非分区表
    props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
    props.put("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor")

    // 写入模式
    props.put("hoodie.datasource.write.operation", "upsert")
    // 设置主键列名
    props.put("hoodie.datasource.write.recordkey.field", "distinct_id")
    // 设置数据更新时的列名
    props.put("hoodie.datasource.write.precombine.field", "uuid")

    props.put("hoodie.commits.archival.batch","2")
    props.put("hoodie.cleaner.commits.retained","2")
    props.put("hoodie.keep.min.commits","3")
    props.put("hoodie.keep.max.commits","4")

    // insert upsert 初始并行度
    props.put("hoodie.insert.shuffle.parallelism", "1")
    props.put("hoodie.upsert.shuffle.parallelism", "1")

    // 设置要同步的hive库名
    props.put("hoodie.datasource.hive_sync.database", "default")
    // 设置要同步的hive表名
    props.put("hoodie.datasource.hive_sync.table", "user")
    // 设置数据集注册或同步到hive metastore
    props.put("hoodie.datasource.hive_sync.enable", "true")
    // 设置jdbc 连接同步
    props.put("hoodie.datasource.hive_sync.jdbcurl", hiveJDBCURL)
    // hive username
    props.put("hoodie.datasource.hive_sync.username", hiveJDBCUsername)


    props

  }





}
