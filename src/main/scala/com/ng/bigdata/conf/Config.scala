package com.ng.bigdata.conf

/**
  * @constructor 命令行参数解析
  * @author QF
  * @date 2020/6/9 4:31 PM
  * @version V1.0
  */
case class Config(
                   env: String = "",
                   brokerList: String = "",
                   sourceTopic: String = "",
                   checkpointDir: String ="",
                   path:String = "",
                   trigger:String = "300",
                   hudiEventBasePath:String ="",
                   hudiUserBasePath:String ="",
                   tableType:String="COW",
                   syncDB:String ="",
                   syncJDBCUrl:String ="",
                   syncJDBCUsername:String ="hive"


                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      opt[String]('b', "brokerList").required().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('t', "sourceTopic").required().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
      programName match {
        case "Log2Console" =>

        case "Log2Hdfs" =>
          opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
          opt[String]('p', "path").required().action((x, config) => config.copy(path = x)).text("data save to hdfs path")
          opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")

        case "Log2Hudi" =>
          opt[String]('i', "trigger").optional().action((x, config) => config.copy(trigger = x)).text("default 300 second,streaming trigger interval")
          opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("hdfs dir which used to save checkpoint")
          opt[String]('g', "hudiEventBasePath").required().action((x, config) => config.copy(hudiEventBasePath = x)).text("hudi event table hdfs base path")
          opt[String]('u', "hudiUserBasePath").required().action((x, config) => config.copy(hudiUserBasePath = x)).text("hudi user table  hdfs base path")
          opt[String]('s', "syncDB").required().action((x, config) => config.copy(syncDB = x)).text("hudi sync hive db")
          opt[String]('y', "tableType").optional().action((x, config) => config.copy(tableType = x)).text("hudi table type MOR or COW. default COW")
          opt[String]('r', "syncJDBCUrl").required().action((x, config) => config.copy(syncJDBCUrl = x)).text("hive server2 jdbc, eg. jdbc:hive2://sparkunique:10000")
          opt[String]('n', "syncJDBCUsername").optional().action((x, config) => config.copy(syncJDBCUsername = x)).text("hive server2 jdbc username, default: hive")
      }
    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
//        println("cannot parse args")
        System.exit(-1)
        null
      }
    }
  }
}