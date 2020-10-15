package com.ng.bigdata.util

import java.sql.{Connection, DriverManager}

import org.slf4j.LoggerFactory

/**
 * @Description: Mysql
 * @Author: QF
 * @Date: 2020/6/11 1:46 PM
 * @Version V1.0
 */

class MysqlUtil private(val config:collection.mutable.Map[String,String]){
  val log = LoggerFactory.getLogger("MysqlUtil")

  def getMysqlConn():Connection={
    var connection:Connection = null
    try {
      Class.forName(config("driver"))
      connection=DriverManager.getConnection(config("url"), config("username"), config("password"))
    }catch {
      case e: Exception => {
        log.error("mysql创建连接错误",e)
        try{
          connection.close()
        }catch {
          case e:Exception=>{
            log.error("mysql关闭连接错误",e)
          }
        }
      }
    }
    connection
  }
}


object MysqlUtil {

  val mysqlConfProd = collection.mutable.Map(
    "driver" -> "com.mysql.jdbc.Driver",
    "url" -> "jdbc:mysql://mysql.yihongyeyan.com:3306/biz?autoReconnect=true",
    "username" -> "qf001",
    "password" -> "QF-common1001-###"
  )

  var mysqlUtil: MysqlUtil = _
  //默认调用 apply方法 进行初始化对象
  def apply() = if (mysqlUtil == null) {mysqlUtil = new MysqlUtil(mysqlConfProd); mysqlUtil} else mysqlUtil



}
