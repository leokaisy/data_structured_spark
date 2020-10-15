package com.ng.bigdata.util

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable

/**
  * @description: 从Mysql业务库中获取meta表信息
  * @author: QF
  * @date: 2020/6/11 1:43 PM
  * @version V1.0
  */
object Meta {

  // 根据meta信息中的字段和字段类型生成样本数据，提供schema
  def getMetaJson(metaType:String ):String= {
    val conn=MysqlUtil().getMysqlConn()
    val statement = conn.createStatement()
    var execSQL=""
    metaType match {
      case "event" =>
        execSQL="select field, field_type from biz.meta where meta_type=0"
      case "user" =>
        execSQL="select field, field_type from biz.meta where meta_type=1"
      case _ =>
        return ""
    }
    val resultSet = statement.executeQuery(execSQL )
    val jsonMeta = new JSONObject()
    while ( resultSet.next() ) {
      val field = resultSet.getString("field")
      val fieldType = resultSet.getString("field_type")
        genSimpleJsonData(jsonMeta,field,fieldType)

    }
    conn.close()

    jsonMeta.toJSONString

  }



  // meta信息中的字段和字段类型转换为map对象
  def getMeta:mutable.HashMap[String,String]= {
    val columnMetaMap= new mutable.HashMap[String,String]
    val conn=MysqlUtil().getMysqlConn()
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery("select field, field_type from biz.meta" )
    val jsonMeta = new JSONObject()
    while ( resultSet.next() ) {
      val field = resultSet.getString("field")
      val fieldType = resultSet.getString("field_type")
      columnMetaMap += (field -> fieldType)
    }
    conn.close()

    columnMetaMap

  }


  // 各个数据类型的样本数据
  def genSimpleJsonData(jsonObj:JSONObject,field:String,fieldType:String ){

    fieldType.toLowerCase match {
      case "string" => jsonObj.put(field,"")
      case "int" =>  jsonObj.put(field,0)
      case "bigint" => jsonObj.put(field,0L)
      case "double" => jsonObj.put(field,0.1)
      case "array" => jsonObj.put(field,List())
    }

  }

  def main(args: Array[String]): Unit = {
    val meta = getMetaJson("event")
    println(meta)
  }
}
