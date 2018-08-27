package com.ibeifeng.senior.usertrack.spark.ad

import java.util.Properties

import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.spark.util.{SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by ibf on 06/11.
  */
object AdClickRealTimeStateSpark {
  // 1. 根据配置信息获取jdbc的连接信息
  lazy val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val username = ConfigurationManager.getProperty(Constants.JDBC_USER)
  lazy val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  lazy val props = {
    val p = new Properties()
    p.put("user", username)
    p.put("password", password)
    p
  }
  // 2. 获取是本地执行还是集群执行
  lazy val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

  def main(args: Array[String]): Unit = {
    // 一、创建上下文
    // 1.1 获取相关的变量
    val appName = Constants.SPARK_APP_NAME_AD
    // 1.2 创建上下文
    val conf = SparkConfUtil.generateSparkConf(appName, isLocal)
    conf.set("spark.streaming.blockInterval", "1s")
    // 1.3 构建SparkContext
    val sc = SparkContextUtil.getSparkContext(conf)
    // 1.4 构建一个StreamingContext对象，专门用于数据读取操作（在实际的工作中这里最好使用HA的方式来创建StreamingContext对象）
    val ssc = new StreamingContext(sc, Seconds(30))
    // 1.5 设置checkpoint的路径(实际工作中一定是HDFS上的路径，我们这里简化为设置为本地的文件系统)
    val checkpointDir = s"/beifeng/spark/project/ad/chk/${System.currentTimeMillis()}"
    ssc.checkpoint(checkpointDir)

    // 二、集成KAFKA数据形成DStream对象(这里使用Use Receiver的方式进行数据读写操作)
    val kafkaParams = Map(
      "zookeeper.connect" -> ConfigurationManager.getProperty(Constants.KAFKA_ZOOKEEPER_URL),
      "group.id" -> appName,
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
      .split(",")
      .map(v => v.split(":"))
      .filter(_.length == 2)
      .map(v => (v(0).trim, v(1).trim.toInt))
      .toMap
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)

    // 三、数据格式转换
    val formatedDStream = this.formatAdRealTimeDStreamData(dstream)

    // 四、黑名单数据过滤
    val filteredDStream = this.filterByBlackList(formatedDStream)

    // 五、黑名单数据动态更新
    this.dynamicUpdateBlockList(filteredDStream)

    // 六、实时累加广告统计量
    val aggrDStream = this.calculateRealTimeState(filteredDStream)

    // 七、获取各个省份Top5的广告点击
    this.calculateProvinceTop5Ad(aggrDStream)

    // 八、统计最近一段时间的广告点击情况
    this.calculateAdClickCountByWindow(filteredDStream)

    // 九、启动Streaming的应用
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 实时统计最近10分钟的广告点击量
    * 使用window分析函数进行操作
    * -1.窗口大小
    * window interval: 60 * 10 = 600s
    * -2. 滑动大小(批次产生的间隔时间)
    * slider interval: 1 * 60 = 60s
    * 需要的字段信息：
    * 时间戳(使用批次时间即可)，广告id，点击次数
    *
    * @param dstream
    */
  def calculateAdClickCountByWindow(dstream: DStream[AdClickRecord]): Unit = {
    // 1. 数据转换
    val mappedDStream = dstream.map(record => (record.adID, 1))
    // 2. 数据聚合
    val aggrDStream = mappedDStream.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (c: Int, d: Int) => c - d,
      Minutes(10),
      Minutes(1)
    )
    // 3. 给定一个时间戳
    val finalDStream: DStream[(String, Int, Int)] = aggrDStream.transform((rdd, time) => {
      // 将time进行时间格式化
      val dateStr = DateUtils.parseLong2String(time.milliseconds, "yyyyMMddHHmmss")
      rdd.map {
        case (adID, count) => (dateStr, adID, count)
      }
    })

    // 4. 结果保存RDBMs
    // TODO: 作业
    finalDStream.print(10)
  }

  /**
    * 获取各个省份top5的热门广告数据
    * 最终需要的字段信息: province adid  count date
    *
    * @param dstream 只包含了一天的数据量
    */
  def calculateProvinceTop5Ad(dstream: DStream[((String, String, String, Int), Long)]): Unit = {
    // 1. 聚合计算广告点击量
    val dailyAndClickCountDStream = dstream
      .map {
        case ((date, province, _, adID), count) => ((date, province, adID), count)
      }
      .reduceByKey(_ + _)

    // 2. 获取每天、各个省份Top5的广告数据 ===> 分组排序TopN的程序
    val top5ProvinceAdClickCountDStream = dailyAndClickCountDStream.transform(rdd => {
      rdd
        .map {
          case ((date, province, adID), count) => {
            ((date, province), (adID, count))
          }
        }
        .groupByKey()
        .flatMap {
          case ((date, province), iter) => {
            // a. 从iter中获取count数量最多的五条数据
            val top5Iter = iter
              .toList
              .sortBy(_._2)
              .takeRight(5)
            // b. 数据合并转换并返回
            top5Iter.map {
              case (adID, count) => ((date, province, adID), count)
            }
          }
        }
    })

    // 3. 数据保持
    // TODO: 作业 写到mysql
    top5ProvinceAdClickCountDStream.print(10)
  }

  /**
    * 实时统计累加广告点击量
    * 实时统计每天、每个广告、每个省份、每个城市下的广告的点击数量
    *
    * @param dstream 过滤黑名单之后的数据
    * @return 返回的结果类型是DStream[((日期,省份,城市,广告id),点击的次数)]
    */
  def calculateRealTimeState(dstream: DStream[AdClickRecord]): DStream[((String, String, String, Int), Long)] = {
    // 1. 将DStream转换为key/value键值对类型的
    val mappedDStream = dstream.map {
      case AdClickRecord(timestamp, province, city, _, adID) => {
        // 将timestamp转换为日期字符串，格式为: yyyyMMdd
        val date = DateUtils.parseLong2String(timestamp, DateUtils.DATEKEY_FORMAT)
        ((date, province, city, adID), 1)
      }
    }
    // 2. 累加统计数据结构值
    val aggrDStream: DStream[((String, String, String, Int), Long)] = mappedDStream
      .reduceByKey(_ + _)
      .updateStateByKey(
        (values: Seq[Int], state: Option[Long]) => {
          // 1. 获取当前key的传递过来的值
          val currentValue = values.sum

          // 2. 获取之前状态保存值
          val preValue = state.getOrElse(0L)

          // 3. 更新状态并返回
          Some(currentValue + preValue)
        }
      )
    // 3. 数据结果输出保存MySQL
    // TODO: 作业自己写
    aggrDStream.print(5)

    // 4. 结果返回
    aggrDStream
  }
  /**
    * 动态更新黑名单用户
    *
    * @param dstream
    */
  def dynamicUpdateBlockList(dstream: DStream[AdClickRecord]): Unit = {
    // 1. 获取黑名单用户
    // window函数要求给定的窗口大小和滑动大小必须是父DStream产生批次间隔时间的整数倍
    val blackListDStream = dstream
      .map(record => (record.userID, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        (c: Int, d: Int) => c - d,
        Minutes(5), // 窗口大小为5分钟，意思是计算最近五分钟的数据
        Minutes(3) // 滑动大小3分钟，意思是每隔三分钟产生一个执行批次任务
      )
      .filter(_._2 > 100)

    // 2. 将结果数据保存到RDBMs中
    blackListDStream.foreachRDD(rdd => {
      // TODO: 这里使用将数据转换为DataFrame的形式进行数据存储操作，自行将其更改为使用foreachPartition进行数据输出
      // 1. 上下文构建
      val sc = rdd.sparkContext
      val sqlContext = SQLContextUtil.getInstance(sc, integratedHive = !isLocal)
      import sqlContext.implicits._
      // 2. 将RDD转换为DataFrame
      val df = rdd.toDF("user_id", "count")
      // 3. dataframe数据保持
      df.write
        .mode(SaveMode.Append)
        .jdbc(url, "tb_black_users", props)
    })
  }


  /**
    * 根据保存在数据库中的黑名单数据进行数据过滤
    *
    * @param dstream
    * @return
    */
  def filterByBlackList(dstream: DStream[AdClickRecord]): DStream[AdClickRecord] = {
    // 先获取数据库中的黑名单数据，然后再进行过滤操作
    dstream.transform(rdd => {
      // 0. 初始化上下文
      val sc = rdd.sparkContext
      val sqlContext = SQLContextUtil.getInstance(sc, integratedHive = !isLocal)
      // 1. 先获取黑名单数据
      // TODO: 作业-->思考能不能不每次都从数据库中加载黑名单用户(前提假设黑名单是数据量比较小)
      val table = "tb_black_users"
      val blackListRDD: RDD[(Int, Int)] = sqlContext
        .read
        .jdbc(url, table, props)
        .map(row => {
          val userID = row.getAs[Int]("user_id")
          val count = row.getAs[Int]("count")
          (userID, count)
        })

      // 2. rdd数据过滤
      rdd
        .map(record => (record.userID, record))
        .leftOuterJoin(blackListRDD)
        .filter(_._2._2.isEmpty)
        .map(_._2._1)
    })
  }

  /**
    * 将string类型中的广告点击数据转换为具体的AdClickRecord对象
    *
    * @param dstream
    * @return
    */
  def formatAdRealTimeDStreamData(dstream: DStream[String]): DStream[AdClickRecord] = {
    // 数据默认分割符是空格，一行数据只有5个字段属性
    /*dstream
      .map(line => line.split(" "))
      .filter(_.length == 5)
      .map(arr => Try(AdClickRecord(arr(0).toLong, arr(1), arr(2), arr(3).toInt, arr(4).toInt)))
      .filter(_.isSuccess)
      .map(_.get)*/

    dstream.mapPartitions(iter => {
      iter.flatMap(line => {
        // 数据分割
        val arr = line.split(" ")
        // 获取返回结果
        val resultIter = Try(AdClickRecord(arr(0).toLong, arr(1), arr(2), arr(3).toInt, arr(4).toInt)) match {
          case Success(record) => {
            // 解析成功
            Iterable(record)
          }
          case Failure(msg) => {
            // 解析失败
            Iterable.empty[AdClickRecord]
          }
        }
        // 返回结果
        resultIter
      })
    })
  }

}

case class AdClickRecord(
                          timestamp: Long,
                          province: String,
                          city: String,
                          userID: Int,
                          adID: Int
                        )
