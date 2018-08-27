package com.ibeifeng.senior.usertrack.spark.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by ibf on 06/11.
  */
object GroupConcatUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("str", StringType, true)))

  override def bufferSchema: StructType = StructType(Array(StructField("buf", StringType, true)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取缓冲区中的值
    val bufferValue = buffer.getString(0)

    // 获取输入的值
    val inputValue = input.getString(0)

    // 合并数据
    val mergedValue = mergeValue(bufferValue, inputValue)

    // 更新缓存区的值
    buffer.update(0, mergedValue)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 获取缓冲区中的值
    var bufferValue1 = buffer1.getString(0)
    val bufferValue2 = buffer2.getString(0)

    // 合并数据
    for (tmpStr <- bufferValue2.split(",")) {
      bufferValue1 = mergeValue(bufferValue1, tmpStr)
    }

    // 更新缓存区
    buffer1.update(0, bufferValue1)
  }


  /**
    * buffer的格式为:"101:上海:1,102:杭州:1,103:南京:1"
    * input的格式为:"101:上海:1"
    *
    * @param buffer
    * @param input
    * @return
    */
  private def mergeValue(buffer: String, input: String): String = {
    if (input == null || input.trim.isEmpty) buffer
    else {
      val inputArr = input.split(":")
      val key = inputArr(0) + ":" + inputArr(1)
      if (buffer.contains(key)) {
        // 需要更新
        buffer
          .split(",")
          .map(v => {
            if (v.contains(key)) {
              // 更新
              s"${key}:${v.split(":")(2).toInt + inputArr(2).toInt}"
            } else {
              // 直接返回
              v
            }
          })
          .mkString(",")
      } else {
        // 增加就可以
        if (buffer.isEmpty) {
          input
        } else {
          buffer + "," + input
        }
      }
    }
  }

  override def evaluate(buffer: Row): Any = buffer.getString(0)
}
