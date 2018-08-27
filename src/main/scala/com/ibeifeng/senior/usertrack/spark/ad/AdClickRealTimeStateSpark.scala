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
import org.apache.spark.streaming.dstream.{DStream, SelfReducedWindowedDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}

/**
  * 一般生产环境下，SparkStreaming的程序都是HA的，而且和kafka集成的时候都选择direct模式
  * Created by ibf on 09/03.
  */
object AdClickRealTimeStateSpark {
  lazy val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
  // kafka中的数据分隔符
  val delimeter = " "

  lazy val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val table = "tb_black_users"
  lazy val props = {
    val props = new Properties()
    props.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    props.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    props
  }

  def main(args: Array[String]): Unit = {
    // 一、上下文的构建
    // 1.1 获取相关变量
    val appName = Constants.SPARK_APP_NAME_AD
    // 1.2 创建上下文配置对象
    val conf = SparkConfUtil.generateSparkConf(appName, isLocal = isLocal)
    // 1.3 SparkContext构建
    val sc = SparkContextUtil.getSparkContext(conf)
    // 1.4 SparkStreaming的构建
    // batchDuration: 指定批次产生间隔时间，一般要求：该值比批次的平均执行执行要大(大5%~10%)
    val ssc = new StreamingContext(sc, Seconds(10))
    // 1.5 设置checkpoint文件夹路径，实际工作中一定为hdfs路径，这里简化为本地磁盘路径
    val path = s"result/checkpoint/ad_${System.currentTimeMillis()}"
    ssc.checkpoint(path)

    // 二、SparkStreaming和Kafka的集成 ===> 使用Receiver的方式
    val kafkaParams = Map(
      "zookeeper.connect" -> ConfigurationManager.getProperty(Constants.KAFKA_ZOOKEEPER_URL),
      "group.id" -> appName
    )
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
      .split(",")
      .map(v => v.split(":"))
      .filter(_.length == 2)
      .map(a => (a(0), a(1).toInt))
      .toMap
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics,
      StorageLevel.MEMORY_AND_DISK_2
    ).map(_._2)


    // 三、数据格式转换(对于kafka中的数据可能需要进行一些转换操作，比如：类型转换)
    val formattedDStream = this.formattedAdRealTimeDStreamData(dstream)

    // 四、黑名单数据过滤
    val filteredDStream = this.filterByBlackList(formattedDStream)

    // 五、黑名单实时/动态更新
    this.dynamicUpdateBlackList(filteredDStream)

    // 六、实时累加统计广告点击量
    /**
      * 实时的统计每天 每个省份 每个城市  每个广告的点击点
      * 数据不涉及到去重操作，有一条算一条
      * ===> 以：日期+省份名称+城市名称+广告id为key，统计数据量的中
      **/
    val aggrDStream = this.calculateRealTimeState(filteredDStream)

    // 七、获取各个省份TOP5的广告点击数据
    this.calculateProvinceTop5Ad(aggrDStream)

    // 八、获取最近一段时间的广告点击数据
    this.calculateAdClickCountByWindow(filteredDStream)

    // 九、启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 目的：是为了看广告的点击趋势的
    * 实时统计最近十分钟的广告点击量
    * 使用window函数进行分析
    * -1. 窗口大小： 指定每个批次执行的时候，处理的数据量是最近十分钟的数据
    * window interval: 60 * 10 = 600s
    * -2. 滑动大小： 指定批次生成的间隔时间为一分钟，即一分钟产生一个待执行的批次
    * slider interval: 1 * 60 = 60s
    *
    * @param dstream
    */
  def calculateAdClickCountByWindow(dstream: DStream[AdClickRecord]): Unit = {
    // 1. 数据转换
    val mappedDStream = dstream.map(record => (record.adID, 1))
    // 2. 窗口分析
    val aggDStream: DStream[(Int, Int)] = mappedDStream.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (c: Int, d: Int) => c - d, // c表示上一个批次的执行结果，d表示上一个批次和当前批次没有重叠的部分的结果数据
      Minutes(10),
      Minutes(1)
    )
    // 3. 为了能够体现点击的趋势变化，直白来讲就是画图的时候可以画一个曲线图(与时间相光的); 所以需要在dstream中的数据中添加一个时间种类的属性 ==> 可以考虑将批次时间添加到数据中
    val finalDStream = aggDStream.transform((rdd, time) => {
      // time即批次的产生时间
      val dateStr = DateUtils.parseLong2String(time.milliseconds, "yyyyMMddHHmmss")
      rdd.map {
        case (adId, count) => (dateStr, adId, count)
      }
    })
    // 4. 数据保存MySQL
    // TODO: 自己完善
    finalDStream.print(5)
  }

  /**
    * 获取各个省份点击次数最多的Top5的广告数据
    * 最终需要的字段信息: 省份、广告id、点击次数、日期
    *
    * @param dstream
    */
  def calculateProvinceTop5Ad(dstream: DStream[((String, String, String, Int), Long)]): Unit = {
    // 1. 计算点击量
    val dailyAndClickCountDStream = dstream
      .map {
        case ((date, province, _, adId), count) => {
          ((date, province, adId), count)
        }
      }
      .reduceByKey(_ + _)
    // 2. 获取各个省份各个日期点击次数最多的前5个广告数据 ==> 分组排序TopN的程序 ===> SparkCore groupByKey + map(flatMap)  / Spark SQL的row_number函数
    val top5ProvinceAdClickCountDStream = dailyAndClickCountDStream.transform(rdd => {
      rdd
        .map {
          case ((date, province, adId), count) => {
            ((date, province), (adId, count))
          }
        }
        .groupByKey()
        .flatMap {
          case ((date, province), iter) => {
            // 从iter中获取出现次数最多的前5个广告数据
            val top5AdIter = iter
              .toList
              .sortBy(_._2)
              .takeRight(5)
            // 结果数据输出转换
            top5AdIter
              .map {
                case (adId, count) => {
                  ((date, province, adId), count)
                }
              }
          }
        }
    })

    // 3. 结果保存数据库
    // TODO: 自己完善
    top5ProvinceAdClickCountDStream.print(5)
  }

  /**
    * 实时的统计每天 每个省份 每个城市  每个广告的点击点
    * 数据不涉及到去重操作，有一条算一条
    *
    * ===> 以：日期+省份名称+城市名称+广告id为key，统计数据量的中
    * @param dstream 过滤之后的数据
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
    * 动态黑名单更新机制
    *
    * @param dstream 过滤之后的数据
    */
  def dynamicUpdateBlackList(dstream: DStream[AdClickRecord]): Unit = {
    // dstream中已经删除过黑名单用户，dstream中的数据可以认为没有已经存在的黑名单用户数据，不需要考虑在这里获取的黑名单用户在以前的批次处理中出现过

    /**
      * 考虑是不是应用上一个批次的执行结果导致当前批次执行失败的???
      * 思考一下reduceByKeyAndWindow这个API的执行方式，将父DStream的多个批次的执行集合成为子DStream的一个批次来执行；在子DStream的批次过程中，如果当前批次和上一个批次有重叠的部分，那么当前批次执行的时候，上一个批次中重叠部分的内容有可能不执行(当执行结果还存在于磁盘/内存中的时候), 反映在DAG图中就是灰色的部分；所以如果有一个用户在上一个批次计算为黑名单，但是对于父DStream而言(数据过滤的DStream)，没有进行重新的运行(数据过滤), 所以就会认为这个用户不是该过滤的数据，从而在当前批次的时候，该数据会重复的计算
      * 解决方案：
      * -1. 在更新黑名单数据之前，将已经存在的黑名单用户过滤掉
      * -2. 将数据的插入的操作更改的Insert Or Update操作
      * -3. 采用我们设置的一种数据代码结构(黑名单数据的更新和数据过滤没有关系), 但是数据更新一定是Insert Or Update
      * -4. 自定义一个Window类型的API
      */

    // 1. 计算当前批次中每个用户的广告点击次数（每三分钟执行一次，每次执行处理最近五分钟的数据）
    /*val combinerDStream = dstream
      .map(record => (record.userID, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => {
          a + b
        },
        Minutes(5), // 窗口大小, 计算最近五分钟的数据
        //        Minutes(3) // 滑动大小, 每三分钟计算一次
        Seconds(30) //  滑动大小, 每30秒计算一次/产生一个批次，这里是为了看黑名单执行情况的
      )*/

    val mappedDStreama = dstream.map(record => (record.userID, 1))
    val combinerDStream = SelfReducedWindowedDStream.reduceByKeyAndWindow(
      (a: Int, b: Int) => {
        if (a < 0) {
          // 当a小于0，表示该key(userid)已经是一个黑名单用户，所以返回-1
          // 表示该数据在上一个批次执行的时候，是黑名单用户
          -1
        } else {
          a + b
        }
      },
      (c: Int, d: Int) => {
        // c的只有两种可能，一种是等于-1，表示该值本身大于100，在上一个批次的时候就是黑名单用户，另外一种就是1~100之间的一个数字，表示上一个批次的该key(userid)的广告点击次数，即该用户不是黑名单用户
        // d是上一个批次和当前批次没有重叠部分的数据结果
        // c-d表示重叠部分的结果，当然如果c-d的结果小于0，表示该key(userid)在上一个批次的时候就是黑名单用户
        c - d
      },
      (c: Int) => {
        if (c > 100) {
          // c为上一个批次对应key的执行结果，当c的值大于100的时候，表示该key(user_id)已经是一个黑名单用户了，而且已经保存到数据库中了
          -1
        } else {
          c
        }
      },
      Minutes(5), // 窗口大小, 计算最近五分钟的数据
      //        Minutes(3) // 滑动大小, 每三分钟计算一次
      Minutes(3), //  滑动大小, 每30秒计算一次/产生一个批次，这里是为了看黑名单执行情况的
      dstream.context,
      mappedDStreama
    )

    // 2. 获取黑名单列表
    val blackDStream = combinerDStream
      .filter(t => t._2 > 100) // 大于100表示新增的黑名单用户
      .transform(rdd => {
        // 白名单用户数据过滤
        // 白名单用户在数据库中，所以首先需要读取白名单用户数据，这里直接创建模拟数据
        val sc = rdd.sparkContext
        val whiteListRDD = sc.parallelize(0 until 1000)
        val broadcastOfWhiteList = sc.broadcast(whiteListRDD.collect())

        // 数据过滤
        // 基于广播变量来进行数据过滤操作
        //        rdd.filter(tuple => !broadcastOfWhiteList.value.contains(tuple._1))
        // 基于join进行数据过滤
        rdd
          .leftOuterJoin(whiteListRDD.map((_, 1)))
          .filter(_._2._2.isEmpty)
          .map(t => (t._1, t._2._1))
      })

    // 3. 黑名单数据保存
    /**
      * DStream的数据保存
      * 要不转换为RDD进行数据保存，要不转换为DataFrame进行数据保存
      * NOTE: 由于在这里计算出来的黑名单用户，是一定在之前的数据中不是黑名单的用户，也就是说数据库中没有该用户的黑名单数据
      */
    blackDStream.foreachRDD(rdd => {
      // NOTE: 由于数据不需要进行Insert Or Update操作，输出到MySQL使用SparkSQL进行数据输出比较简单，故这里采用DataFrame的write进行数据输出
      // 将rdd转换为DataFrame
      val sc = rdd.sparkContext
      val sqlContext = SQLContextUtil.getInstance(sc, false)
      import sqlContext.implicits._
      val df = rdd.toDF("user_id", "count")
      try {
        df.write.mode(SaveMode.Append).jdbc(url, table, props)
      } catch {
        case e: Exception => {
          // 异常打印，然后加一个断点，进行debug
          println(e.getMessage)
          throw e
        }
      }
    })
  }

  /**
    * 根据保存在数据库中的黑名单数据进行数据过滤操作
    *
    * @param dstream
    * @return
    */
  def filterByBlackList(dstream: DStream[AdClickRecord]): DStream[AdClickRecord] = {
    dstream.transform(rdd => {
      // 一、读取RDBMs中的黑名单数据
      val blackListRDD: RDD[(Int, Int)] = {
        val sc = rdd.sparkContext
        val sqlContext = SQLContextUtil.getInstance(sc, false)
        sqlContext
          .read
          .jdbc(url, table, props)
          .map(row => {
            val userId = row.getAs[Int]("user_id")
            val count = row.getAs[Int]("count")
            (userId, count)
          })
      }

      // 二、数据过滤
      rdd
        .map(record => (record.userID, record))
        .leftOuterJoin(blackListRDD)
        .filter(_._2._2.isEmpty)
        .map(_._2._1)
    })
  }


  /**
    * 将String类型转换为方便操作的数据类型. 数据的格式化操作
    *
    * @param dstream
    */
  def formattedAdRealTimeDStreamData(dstream: DStream[String]): DStream[AdClickRecord] = {
    /*dstream
      .map(line => {
        val arr = line
          .split(delimeter)
          .map(_.trim)
          .filter(_.nonEmpty)
        if (arr.length == 5) {
          // 数据格式正确
          Try {
            AdClickRecord(
              arr(0).toLong,
              arr(1),
              arr(2),
              arr(3).toInt,
              arr(4).toInt
            )
          }
        } else {
          // 数据格式不正确
          Failure(new IllegalArgumentException("Kafka中数据格式异常"))
        }
      })
      .filter(_.isSuccess)
      .map(_.get)*/

    /*dstream.mapPartitions(iter => {
      iter.map(line => {
        val arr = line
          .split(delimeter)
          .map(_.trim)
          .filter(_.nonEmpty)
        if (arr.length == 5) {
          // 数据格式正确
          Try {
            AdClickRecord(
              arr(0).toLong,
              arr(1),
              arr(2),
              arr(3).toInt,
              arr(4).toInt
            )
          }
        } else {
          // 数据格式不正确
          Failure(new IllegalArgumentException("Kafka中数据格式异常"))
        }
      })
        .filter(_.isSuccess)
        .map(_.get)
    })*/

    dstream.flatMap(line => {
      val arr = line
        .split(delimeter)
        .map(_.trim)
        .filter(_.nonEmpty)


      Try {
        AdClickRecord(
          arr(0).toLong,
          arr(1),
          arr(2),
          arr(3).toInt,
          arr(4).toInt
        )
      } match {
        case Success(record) => {
          Iterator.single(record).toIterable
        }
        case Failure(exception) => {
          // TODO: 这里可以进行异常代码处理
          Iterable.empty[AdClickRecord]
        }
      }
    })
    // TODO: 有时候会考虑将多个连续的窄依赖的API进行合并，可以减少rdd的构建次数，多于比较复杂的业务有一定的性能提升
  }
}
