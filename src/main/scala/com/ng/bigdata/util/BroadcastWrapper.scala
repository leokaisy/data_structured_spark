package com.ng.bigdata.util

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

// 封装的广播变量，在DStream的foreachRDD中可用于更新广播变量,也可以开一个后台线程，定时更新广播变量
// 注：本项目中当前并未使用次类
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val sc: SparkContext,
                                          @transient private val _v: T) {

  @transient private var v = sc.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    v.unpersist(blocking)
    v = sc.broadcast(newValue)
  }

  def value: T = v.value

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
}