package com.ibeifeng.senior.usertrack.spark.session

import java.sql.Connection

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.dao.factory.DAOFactory
import com.ibeifeng.senior.usertrack.jdbc.JDBCHelper
import com.ibeifeng.senior.usertrack.mock.MockDataUtils
import com.ibeifeng.senior.usertrack.spark.util.{JSONUtil, SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.DateUtils.DateTypeEnum
import com.ibeifeng.senior.usertrack.util.{DateUtils, ParamUtils, StringUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by ibf on 06/10.
  */
object UserVisitSessionAnalyzerSpark {
      def main(args: Array[String]): Unit = {
        //一、前期处理
        //1.1根据传入的taskID获取对应的任务的过滤参数
        //a.获取taskID
        val taskID = ParamUtils.getTaskIdFromArgs(args)
        //b.从数据库中获取任务对象
        val task  = if(taskID == null){
          //参数异常
          throw  new IllegalArgumentException("taskid参数异常")
        }else{
      //读取数据库的数据
      /**
       * 使用第三方框架:mybatis,hibernate.....
       * Dao模式，jdbcHelp来读取数据
       */
      //获取dao对象
      val taskDao = DAOFactory.getTaskDAO
      //获取task的对象
      taskDao.findByTaskId(taskID)
    }
    //c.task对象获取过滤参数
    val taskParam = ParamUtils.getTaskParam(task)
    if(taskParam == null || taskParam.isEmpty){
      throw  new IllegalArgumentException("获取任务过滤参数失败")
    }


        //二、上下文的构建
        //2.1 获取相关的变量
        //应用的前缀名称+ID
        val appName = Constants.SPARK_APP_NAME_SESSION+taskID
        //判断是否在本地运行
        val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
        //2.2 创建上下文
        val conf = SparkConfUtil.generateSparkConf(appName,isLocal)
        //2.3 构建SparkContext
        val sc = SparkContextUtil.getSparkContext(conf)
        //2.4 构建SQLContext,并且构建模拟数据
        val sqlContext = SQLContextUtil.getInstance(sc,
        integratedHive = !isLocal,
        generateMockData = (sc,sqlContext) => {
          MockDataUtils.mockData(sc,sqlContext)
        })

         //三、数据按照session粒度进行聚合
        //想要得到（sessionid,iter(row1,row2,row3.....)）
        // 3.1 读取数据(根据过滤参数来读取数据), 形成一个RDD
        val actionRDD: RDD[UserVisitSessionRecord] = this.getActionRDDByFilter(sqlContext, taskParam)
        //数据类型转换 RDD[UserVisitSessionRecord]  ==> (sessionid,UserVisitSessionRecord)
             // ===>(String, Iterable[UserVisitSessionRecord])
        val sessionID2RecordsRDD: RDD[(String, Iterable[UserVisitSessionRecord])] = actionRDD.map(record => {
          (record.sessionId,record)
        }).groupByKey()
      //缓存
        sessionID2RecordsRDD.cache()

        // 四、需求一代码
        /**
         * 用户的session聚合统计
         * 主要统计两个指标：会话数量&会话长度
         * 会话数量：sessionID的数量
         * 会话长度：一个会话中，最后一条访问记录的时间-第一条记录的访问数据
         * 具体的指标：
         * 1. 总的会话个数：过滤后RDD中sessionID的数量
         * 2. 总的会话长度(单位秒): 过滤后RDD中所有session的长度的和
         * 3. 无效会话数量：会话长度小于1秒的会话id数量
         * 4. 各个不同会话长度区间段中的会话数量
         * 0-4s/5-10s/11-30s/31-60s/1-3min/3-6min/6min+ ==> A\B\C\D\E\F\G
         * 5. 计算各个小时段的会话数量
         * 6. 计算各个小时段的会话长度
         * 7. 计算各个小时段的无效会话数量
         *
         * 注意：如果一个会话中，访问数据跨小时存在，eg：8:59访问第一个页面,9:02访问第二个页面；把这个会话计算在两个小时中(分别计算)
         **/
        //总会话数量
        val totalSessionCount: Long = sessionID2RecordsRDD.count()
        //每个session的访问时间长度
        val sessionID2LengthRDD: RDD[(String, Long)] = sessionID2RecordsRDD.map {
          case (sessionID, records) => {
            // 1. 获取当前会话中的所有数据的访问时间戳(毫米级)
            val actionTimestamps: Iterable[Long] = records.map(record => {
              // actionTime格式为yyyy-MM-dd HH:mm:ss的时间字符串
              val actionTime = record.actionTime
              val timestamp = DateUtils.parseString2Long(actionTime)
              timestamp
            })

            // 2. 获取第一条数据的时间戳(最小值)和最后一条数据的时间戳(最大值)
            val minActionTimestamp = actionTimestamps.min
            val maxActionTimestamp = actionTimestamps.max
            val length = maxActionTimestamp - minActionTimestamp

            // 3. 返回结果
            // TODO: 这里的sessionID是否需要进行输出操作？？？ ===>  不需要的
            (sessionID, length)
          }
        }
        sessionID2LengthRDD.cache()
        //总会话时间长度
        val totalSessionLength: Double = sessionID2LengthRDD.map(_._2).sum() / 1000
        //总无效会话
        val invalidSessionCount: Long = sessionID2LengthRDD.filter(_._2 < 1000).count()
        val preSessionLengthLevelSessionCount = sessionID2LengthRDD
          .map {
          case (_, length) => {
            // 根据会话长度得到当前会话的级别
            val sessionLevel = {
              if (length < 5000) "A"
              else if (length < 11000) "B"
              else if (length < 31000) "C"
              else if (length < 60000) "D"
              else if (length < 180000) "E"
              else if (length < 360000) "F"
              else "G"
            }

            // 返回结果
            (sessionLevel, 1)
          }
        }
          .reduceByKey(_ + _)
          //其实我们在写spark代码的时候，要注意
          //什么情况下才会有action（实际运算得到结果）
          //如果没有触发action的操作，却直接执行代码中某个值的打印，有可能出现异常
          .collect()
        sessionID2LengthRDD.unpersist()

        // 计算各个小时段各个会话的会话长度
        val dayAndHour2SessionLengthRDD: RDD[((String, Int), Long)] = sessionID2RecordsRDD.flatMap {
          case (sessionID, records) => {
            //1. 获取当前会话中的记录操作对应的时间
            val dayAndHourAndSessionID2TimestampIter: Iterable[((String, Int, String), Long)] = records.map(record => {
              // actionTime格式为yyyy-MM-dd HH:mm:ss的时间字符串
              val actionTime = record.actionTime
              val timestamp = DateUtils.parseString2Long(actionTime)
              // 获取得到day和hour两个时间值
              val day = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
              val hour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)
              ((day, hour, record.sessionId), timestamp)
            })

            // 2. 计算各个时间段各个会话中的会话长度
            val dayAndHour2LengthIter = dayAndHourAndSessionID2TimestampIter
              .groupBy(_._1)
              .map {
              case ((day, hour, sessionID), iter) => {
                val times = iter.map(_._2)
                // a. 获取最大值和最小值
                val maxTimes = times.max
                val minTimes = times.min
                // b. 计算长度
                val length = maxTimes - minTimes
                // c. 返回结果
                ((day, hour), length)
              }
            }

            // 3. 返回结果
            dayAndHour2LengthIter
          }
        }
        dayAndHour2SessionLengthRDD.cache()
        //每天的每个小时的会话计数
        val preDayAndHourOfSessionCount = dayAndHour2SessionLengthRDD
          .map(tuple => (tuple._1, 1))
          .reduceByKey(_ + _)
          .collect()
        val preDayAndHourOfSessionLength: Array[((String, Int), Long)] = dayAndHour2SessionLengthRDD
          .reduceByKey(_ + _)
          .map(tuple => (tuple._1, tuple._2 / 1000))
          .collect()
        val preDayAndHourOfInvalidSessionCount = dayAndHour2SessionLengthRDD
          .filter(t => t._2 < 1000)
          .map(tuple => (tuple._1, 1))
          .reduceByKey(_ + _)
          .collect()
        dayAndHour2SessionLengthRDD.unpersist()

        this.saveSessionAggrResult(sc, taskID, totalSessionCount, totalSessionLength, invalidSessionCount, preSessionLengthLevelSessionCount, preDayAndHourOfSessionCount, preDayAndHourOfSessionLength, preDayAndHourOfInvalidSessionCount)

        // 六、需求二实现
        /**
         * 按照给定的比例从每个小时区间段中抽取session的数据(详细数据)，并且将数据保存到一个HDFS上的文本文件中，同时将每个小时抽取出来的session中的访问次数最多的前10个session的详细信息输出到RDBMs中
         * 1. 获得/抽取最终session的sessionID；抽取的sessionID按照小时进行分布的
         * 2. 将抽取得到的sessionID和session具体信息进行join操作，得到最终的抽取session的详细会话信息
         * 3. 结果保存HDFS
         * 4. 从抽取结果中获取每个小时段访问次数最多的10个session将数据保存到JDBC中
         * 备注：这里的小时段考虑天和小时
         * eg:
         * 一天的session数量10万
         * 给定的比率是1%(1000个session)
         * -1. 先计算每个小时段的session数量
         * 0-1: 1000个
         * .....
         * 9-10: 20000个
         * ......
         * -2. 计算出每个小时段需要被抽取的最少session数量
         * 0-1: 1个
         * ....
         * 9-10: 200个
         * ......
         * -3. 按照给定的数量量随机获取session的数据
         * 功能：为了方便观察者对每个会话的访问轨迹有一定的了解，可以从中得到一些用户的操作习惯，对产生的优化有一定的帮助；而且观察整体的数据，由于数据量太大，很难得到一个结果，但是随机抽样的形式可以保证最终结果的公平性
         **/

        /**
         * 功能的另一种实现：
         *  1、计算每天每小时的记录数量
         *  2、计算每天每小时的session数量
         *  3、通过百分比（ratio）计算每天每小时需要抽取的session条数
         *  4、按时间比例随机抽取算法，计算出每天每小时要抽取的session的索引
         *  5、遍历每天每小时的session，保留想要的数据
         */

        // 从任务参数中获取比率
        val ratio: Double = {
          // 获取参数
          val param: Option[String] = ParamUtils.getParam(taskParam, Constants.PARAM_SESSION_RATIO)
          // Option => Some & None ==> 表示有值无值的
          // Try => Success & Failure ==> 表示执行成功和执行异常
          // value值：-1表示param没有设值，-2表示设的值不是double数据类型，大于0表示正常值
          var value = Try(if (param.isDefined) param.get.toDouble else -1.0).getOrElse(-2.0)

          // 当设置的比例值超过范围的时候(0,0.5]，设置为默认值0.1(10%)
          if (value <= 0 || value > 0.5) {
            value = 0.1
          }
          // 返回结果
          value
        }


        // 创建最基本的数据抽样原始RDD对象，类型: RDD[((day,hour), sessionID]
        val baseSampleDataRDD: RDD[((String, Int), String)] = sessionID2RecordsRDD.flatMap {
          case (sessionID, records) => {
            // 获取当前记录中操作的时间值，返回结果是:(day, hour)
            val day2HourIter: Iterable[(String, Int)] = records.map(record => {
              val actionTime = record.actionTime
              val timestamp = DateUtils.parseString2Long(actionTime)
              // 获取day(yyyy-MM-dd)和hour(小时数，24小时制)
              val day = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
              val hour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)

              // 返回结果
              (day, hour)
            })

            // 数据转换并进行输出
            day2HourIter.map(tuple => (tuple, sessionID))
          }
        }
        baseSampleDataRDD.cache()
        //1、计算每天每小时的记录数量
        val preDateHourCountRecordMap: Map[(String, Int), Long] = baseSampleDataRDD.countByKey()



        //2、计算每天每小时的session数量（并将这个数据转换成Map）
        //获取过滤session后的sessionRDD，做cache，因为后面要使用
       val distinctSessionRDD: RDD[(String, Int, String)] = baseSampleDataRDD
          .map(t =>(t._1._1,t._1._2,t._2))
          .distinct()
        distinctSessionRDD.cache()
        val preDateHourCountSessionMap: Predef.Map[(String, Int), Int] = distinctSessionRDD
          .map{
          case (date,hour,session) =>{
            ((date,hour),1)
          }
        }.reduceByKey(_+_).collect().toMap
        //3、通过百分比（ratio）计算每天每小时需要抽取的session条数
        val needextractCountSessionMap: Predef.Map[(String, Int), Long] = preDateHourCountSessionMap.map(t => {
          //做了一个四舍五入，因为抽取的数据条数只能是整数
          (t._1,if(t._2 > 0 && t._2 < 1)1 else Math.round(t._2*ratio))
        })

        //4、按时间比例随机抽取算法，计算出每天每小时要抽取的session的索引
        //创建Map集合类型 Map[date, mutable.Map[hour, ArrayBuffer[Long]]]
        // ArrayBuffer[Long]里就是每个小时需要抽取的session的下标
        val needExtractsessionIndexMap: mutable.Map[String, mutable.Map[Int, ArrayBuffer[Long]]] = scala.collection.mutable.Map[String, mutable.Map[Int,ArrayBuffer[Long]]]()
        //对needExtractsessionIndexMap集合进行处理，获得每天每小时要抽取session的索引
        needextractCountSessionMap.map {
          case ((date, hour), num) => {
            //获得当天的map
            //getOrElseUpdate有的话就获取，没有的话就添加
            val dateMap = needExtractsessionIndexMap.getOrElseUpdate(date, mutable.Map[Int, ArrayBuffer[Long]]())
            //获得当天当个小时的ArrayBuffer[Long]()
            val extractIndexArr: ArrayBuffer[Long] = dateMap.getOrElseUpdate(hour, ArrayBuffer[Long]())
            //通过循环获得要抽取的随机值
            //构建一个i值为0，用来循环
            var idex = 0
            while (idex < num) {
              //获得某天某小时需要抽取多少数据(num)，创建一个变长数组来存放每次随机出来的index

              //extractIndex是每次生成的随机数
              var extractIndex = Random.nextInt((num / ratio).toInt)
              while (extractIndexArr.contains(extractIndex)) {
                //但是有可能随机出来的值可能是一样的，
                // 如果随机出来的值默认已经存在，就重新生成一次
                extractIndex = Random.nextInt((num / ratio).toInt)
              }
              extractIndexArr += extractIndex
              idex += 1
            }
          }
        }
        //将抽取数据的索引Map做广播变量，广播出去
        val broadCastNeedExtractsessionIndexMap = sc.broadcast(needExtractsessionIndexMap)

        //5、遍历每天每小时的session（去过重的数据）,保留想要的数据
       val arrBaseSampleDataRDD = distinctSessionRDD
          .map{
          case (day,hour,sessionid) => {
            ((day,hour),sessionid)
          }
        }.groupByKey()

        //6、根据广播变量Map抽取符合索引的sessionid
        val extractSession: RDD[(String, (String, Int))] = arrBaseSampleDataRDD.flatMap{
         case ((date,hour),iter) => {
            val it= iter.iterator
            val extractIndexArr:ArrayBuffer[Long] = broadCastNeedExtractsessionIndexMap.value.get(date).get.get(hour).get
           //创建一块空间来存放抽取到的sessionid
           val extractSessionArr:ArrayBuffer[String] = ArrayBuffer[String]()
           //这个是迭代器的迭代次数
           var index = 0
            while (it.hasNext){
              //每次迭代获取到的sessionid
             val sessionID =  it.next()
              //判断这个迭代的次数是否符合抽取的数据的索引
              if(extractIndexArr.contains(index)){
                //符合就将这个sessionid放入到
                extractSessionArr += sessionID
              }
              //迭代次数+1
              index += 1
            }
           //返回抽取到的sessionid
           extractSessionArr.map(sessionid => (sessionid,(date,hour)))
         }
        }
        //7、将抽取到的sessionid的信息字段补全，返回UserVisitSessionRecord对象
        //但是这里可能会将这个对象的全部的信息拿到，所以做一次filter
        val finalSampleSessionRDD: RDD[((String, String, Int), Iterable[UserVisitSessionRecord])] = extractSession.leftOuterJoin(sessionID2RecordsRDD)
          .map{
          case (sessionid,((day,hour),opt)) => {
            val records: Iterable[UserVisitSessionRecord] = opt.get
            val needday: String = day
            val needhour: Int = hour
            val filteredRecords = records.filter(record => {
              val timestamp = DateUtils.parseString2Long(record.actionTime)
              val tmpday = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
              val tmphour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateTypeEnum.HOUR)
              needday.equals(tmpday) && needhour==tmphour
            })
            ((sessionid, day, hour), filteredRecords)
            }
        }

        finalSampleSessionRDD.cache()
        finalSampleSessionRDD.repartition(1).saveAsTextFile(s"beifeng/spark/sample/${System.currentTimeMillis()}")

        finalSampleSessionRDD.foreachPartition(iter => {
          // TODO: 输出到RDBMs中的时候，需要考虑数据提交的方式是什么？到底是批量提交(进行代码修改)还是输出一条就提交一次(自动/默认)
          // 1. 获取JDBC连接
          val jdbcHelper = JDBCHelper.getInstance()
          var conn: Connection = null

          try {
            conn = jdbcHelper.getConnection
            // 2. 创建Statement对象
            val sql = "INSERT INTO tb_task_result_sample_session(`task_id`,`session_id`,`day`,`hour`,`record`) VALUES(?,?,?,?,?)"
            val pstmt = conn.prepareStatement(sql)
            // 3. 对数据进行迭代输出操作
            iter.foreach {
              case ((sessionID, day, hour), records) => {
                val jsonArray: JSONArray = records
                  .map(record => record.transform2JSONObject())
                  .foldLeft(new JSONArray())((arr, obj) => {
                  arr.add(obj)
                  arr
                })

                // 设置数据输出
                // TODO: 对于taskID是否广播变量关系不大，因为taskID只是一个long类型的数据
                pstmt.setLong(1, taskID)
                pstmt.setString(2, sessionID)
                pstmt.setString(3, day)
                pstmt.setInt(4, hour)
                pstmt.setString(5, jsonArray.toJSONString)

                // 由于一个会话中的数据可能会比较大，所以直接提交，不进行batch批量提交
                pstmt.executeUpdate()
              }
            }
            } finally {
            // 4. 进行连接关闭操作
            jdbcHelper.returnConnection(conn)
          }
        })

        //false表示是否阻塞到该RDD的缓存数据全部删除，默认是true
        baseSampleDataRDD.unpersist(false)
        distinctSessionRDD.unpersist(false)
        finalSampleSessionRDD.unpersist(false)



        // 七、需求三: 获取点击、下单、支付次数前10的各个品类的各种操作的次数
        /**
         * 点击、下单、支付是三种不同的操作，需求获取每个操作中触发次数最多的前10个品类(最多30个品类)
         * 对数据先按照操作类型进行分组，然后对每组数据进行计数统计，最后对每组数据进行Top10的结果获取
         * 这个需求实质上就是一个分组排序TopK的需求
         * 步骤:
         * a. 从原始RDD中获取计算所需要的数据
         * ==> 点击的falg为0；下单为1；支付为2
         * b. 求各个品类被触发的次数==>wordcount
         * c. 分组TopN程序-->按照flag进行数据分区，然后对每个分区的数据进行数据获取
         * TODO: 作业 --> 考虑分组TopN实现过程中，OOM异常的解决代码 & 考虑一下使用SparkSQL如何使用
         * d. 由于分组TopN后RDD的数据量直接降低到30条数据一下, 所以将分区数更改为1
         * e. 按照品类id合并三类操作被触发的次数（按理来讲，应该按照categoryID进行数据分区，然后对每组数据进行聚合 => RDD上的操作; 但是由于只有一个分区，调用groupByKeyAPI会存在shuffle过程，这里不太建议直接在rdd上使用groupByKey api; 直接使用mapPartitions， 然后对分区中的数据迭代器进行操作《存在一个分组合并结果的动作》）
         */
        //                                  商品品类id 点击  下单  支付
        val top10CategoryIDAndCountRDD: RDD[(String, (Int, Int, Int))] = sessionID2RecordsRDD
          .flatMap {
          case (sessionID, records) => {
            // 从records迭代器中获取各个操作触发的品类id
            val iter: Iterable[(String, Int)] = records.flatMap(record => {
              val clickCategoryID = record.clickCategoryId
              val orderCategoryIDs = record.orderCategoryIds
              val payCategoryIDs = record.payCategoryIds

              if (StringUtils.isNotEmpty(clickCategoryID)) {
                Iterator.single((clickCategoryID, 0))
              } else if (StringUtils.isNotEmpty(orderCategoryIDs)) {
                orderCategoryIDs
                  .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                  .filter(_.trim.nonEmpty)
                  .map(id => (id, 1))
              } else if (StringUtils.isNotEmpty(payCategoryIDs)) {
                payCategoryIDs
                  .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                  .filter(_.trim.nonEmpty)
                  .map(id => (id, 2))
              } else {
                Iterator.empty
              }
            })
            // 返回值
            iter.map(key => {
              val k: (String, Int) = key
              (key, 1)})
          }
        }
          //key(品类id,flag) value:次数
          .reduceByKey(_ + _)
          //把操作类型做key，然后品类id和次数作为key
          .map(tuple => (tuple._1._2, (tuple._1._1, tuple._2)))
          .groupByKey()
          //这里聚合完就三种类型，点击0，下单1，支付2
          .flatMap {
          case (flag, iter) => {
            // 对iter中的数据按照次数count进行排序；然后获取数量最多的前10个数据
            val top10Category: List[(String, Int)] = iter
              .toList
              .sortBy(_._2)
              .takeRight(10)

            // 结果输出返回
            top10Category.map {
              case (categoryID, count) => {
                (categoryID, (flag, count))
              }
            }
          }
        }
          //三种类型，每种topN，最多就是30条数据，所以全部放到一个分区里
          .repartition(1)
          .mapPartitions(iter => {
          iter
            .toList
            .groupBy(_._1)
            .map {
            case (categoryID, list) => {
              // 对list中的数据进行合并=>list中最多三条数据， 三条数据的flag都不一样
              val categoryCount = list.foldLeft((0, 0, 0))((a, b) => {
                b._2._1 match {
                  case 0 => (b._2._2, a._2, a._3)
                  case 1 => (a._1, b._2._2, a._3)
                  case 2 => (a._1, a._2, b._2._2)
                }
              })

              // 返回结果
              (categoryID, categoryCount)
            }
          }
            .toIterator
        })
        top10CategoryIDAndCountRDD.cache()

        // 数据输出到JDBC
        top10CategoryIDAndCountRDD.foreachPartition(iter => {
          val jdbcHelper = JDBCHelper.getInstance()
          Try {
            val conn = jdbcHelper.getConnection
            val oldAutoCommit = conn.getAutoCommit
            //设置不自动提交
            conn.setAutoCommit(false)
            // 2. 创建Statement对象
            val sql = "INSERT INTO tb_task_top10_category(`task_id`,`category_id`,`click_count`,`order_count`,`pay_count`) VALUES(?,?,?,?,?)"
            val pstmt = conn.prepareStatement(sql)
            var recordCount = 0
            // 3. 对数据进行迭代输出操作
            iter.foreach {
              case (categoryID, (clickCount, orderCount, payCount)) => {
                // 设置数据输出
                pstmt.setLong(1, taskID)
                pstmt.setString(2, categoryID)
                pstmt.setInt(3, clickCount)
                pstmt.setInt(4, orderCount)
                pstmt.setInt(5, payCount)

                // 启动批量提交
                pstmt.addBatch()
                recordCount += 1

                if (recordCount % 500 == 0) {
                  pstmt.executeBatch()
                  conn.commit()
                }
              }
            }
            // 4. 进行连接关闭操作
            pstmt.executeBatch()
            conn.commit()

            // 5. 返回结果
            (oldAutoCommit, conn)
          } match {
            case Success((oldAutoCommit, conn)) => {
              // 执行成功
              Try(conn.setAutoCommit(oldAutoCommit))
              Try(jdbcHelper.returnConnection(conn))
            }
            case Failure(execption) => {
              // 执行失败
              jdbcHelper.returnConnection(null)
              throw execption
            }
          }
        })




        // 八、需求四：获取Top10品类中各个品类被触发的Session中，Session访问数量最多的前10
        /**
         * 触发：点击、下单、支付三种操作中的任意一种
         * 如果在一个会话中，某种操作被触发的多次，那么就计算多次，不涉及数据去重
         * 最终结果：sessionID和各种操作的触发次数报道到JDBC中；同时将具体的session数据保存HDFS
         * 步骤：
         * a. 计算获取Top10的session信息(包含：ID和触发次数)
         * b. 结果保存
         * c. 具体session信息获取，并保存HDFS
         **/
        // 1. 将Top10的categoryID广播出去
        val top10CategoryIDList = top10CategoryIDAndCountRDD.collect().map(t => t._1)
        val broadcastOfTop10CategoryID = sc.broadcast(top10CategoryIDList)
        // 2. 获取Top10的Session信息
        val preCategoryTop10SessionID2CountRDD: RDD[((String, Int), List[(String, Int)])] = sessionID2RecordsRDD
          .mapPartitions(iter => {
          val bv = broadcastOfTop10CategoryID.value
          iter.flatMap {
            case (sessionID, records) => {
              // 求得有效的品类id和类型
              val categoryIDAndFlagIter: Iterable[(String, Int)] = records
                .flatMap(record => {
                val clickCategoryID = record.clickCategoryId
                val orderCategoryIDs = record.orderCategoryIds
                val payCategoryIDs = record.payCategoryIds

                if (StringUtils.isNotEmpty(clickCategoryID)) {
                  Iterator.single((clickCategoryID, 0))
                } else if (StringUtils.isNotEmpty(orderCategoryIDs)) {
                  orderCategoryIDs
                    .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                    .filter(_.trim.nonEmpty)
                    .map(id => (id, 1))
                } else if (StringUtils.isNotEmpty(payCategoryIDs)) {
                  payCategoryIDs
                    .split(Constants.SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE)
                    .filter(_.trim.nonEmpty)
                    .map(id => (id, 2))
                } else {
                  Iterator.empty
                }
              })
                .filter {
                case (categoryID, _) => bv.contains(categoryID)
              }
              // 求被触发的次数(在当前会话中)
              val categoryIDAndFalg2CountIter: Iterator[((String, Int), Int)] = categoryIDAndFlagIter
                .groupBy(v => v)
                .map(tuple => (tuple._1, tuple._2.size))
                .toIterator
              // 返回结果
              categoryIDAndFalg2CountIter.map {
                case ((categoryID, flag), count) => {
                  ((categoryID, flag), (sessionID, count))
                }
              }
            }
          }
        })
          .groupByKey()
          .map {
          case ((categoryID, flag), iter) => {
            val top10SessionID2CountIter = iter
              .toList
              .sortBy(_._2)
              .takeRight(10)

            ((categoryID, flag), top10SessionID2CountIter)
          }
        }
        preCategoryTop10SessionID2CountRDD.cache()

        // 3. 将数据输出到JDBC中（结果数据按照categoryID进行聚合，数据输出到MySQL）
        preCategoryTop10SessionID2CountRDD
          .map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))
          .groupByKey()
          .map {
          case (categoryID, iter) => {
            val categorySessions = iter.foldLeft(("", "", ""))((a, b) => {
              val sessionIDStr = b._2
                .foldLeft(new JSONArray())((aa, bb) => {
                val obj = new JSONObject()
                obj.put("sessionid", bb._1)
                obj.put("count", bb._2)
                aa.add(obj)
                aa
              })
                .toJSONString

              b._1 match {
                case 0 => (sessionIDStr, a._2, a._3)
                case 1 => (a._1, sessionIDStr, a._3)
                case 2 => (a._1, a._2, sessionIDStr)
              }
            })

            // (categoryID, (点击操作会话，下单操作会话，支付操作会话))
            (categoryID, categorySessions)
          }
        }
          .foreachPartition(iter => {
          // TODO: 作业将数据输出到表tb_task_top10_category_session
          iter.take(2).foreach(println)
        })

        // 4. 获取具体会话信息(根据会话id来获取)
        val preCategoryTop10SessionDataSavePath = s"/beifeng/spark-project/top10_category_session/task_${taskID}"
        FileSystem.get(sc.hadoopConfiguration).delete(new Path(preCategoryTop10SessionDataSavePath), true)
        val preCategoryTop10SessionId = preCategoryTop10SessionID2CountRDD
          .flatMap(t => t._2.map(_._1))
          .collect()
          .distinct
        val broadCastOfTop10SessionId = sc.broadcast(preCategoryTop10SessionId)
        sessionID2RecordsRDD
          .filter(t => broadCastOfTop10SessionId.value.contains(t._1))
          .flatMap {
          case (sessionID, records) => {
            records.map(record => record.transform2JSONObject().toJSONString)
          }
        }
          .saveAsTextFile(preCategoryTop10SessionDataSavePath)

        top10CategoryIDAndCountRDD.unpersist()
        preCategoryTop10SessionID2CountRDD.unpersist()
        broadCastOfTop10SessionId.unpersist(true)
        broadcastOfTop10CategoryID.unpersist(true)

        // 八、操作完成后，进行必要的清理操作
        sessionID2RecordsRDD.unpersist()

        Thread.sleep(10000000)
      }


  /**
   * 根据给定的需要抽样的数据量，从给定的迭代器中抽取数据
   *
   * @param iter           原始的数据迭代器
   * @param count          需要抽样的数据
   * @param exclusiveItems 给定需要排除的数据集合
   * @param random         数据随机器
   * @return
   */
  def fetchSampleItem[T](iter: Iterable[T], count: Int, exclusiveItems: Array[T] = Array.empty, random: Random = Random): Iterable[T] = {
    if (count == 0) {
      // 当需要获取的数据为0个的时候，直接返回一个空的迭代器对象
      Iterable.empty[T]
    } else {
      // 1. 数据过滤
      val filteredIter = iter.filterNot(t => exclusiveItems.contains(t))
      // 2. 获取迭代器中的数据的数量
      val iterSize = filteredIter.size
      // 2. 进行数据判断，如果原来的count值比现在这个值小，那没有其他数据了，所以直接返回
      if (iterSize <= count || iterSize == 0) {
        // 直接返回原本的迭代器
        filteredIter
      } else {
        // 从迭代器中进行随机抽象
        // 2.1 随机产生最终数据所在的index下标
        import scala.collection.mutable
        val indexSet = mutable.Set[Int]()
        while (indexSet.size < count) {
          // 随机一个下标保存到集合中
          val index = random.nextInt(iterSize)
          indexSet += index
        }

        // 2.2 从迭代器中获取对应下标的数据
        filteredIter
          .zipWithIndex
          .filter {
          case (item, index) => {
            indexSet.contains(index)
          }
        }
          .map(_._1)
      }
    }
  }




/**
   * 保存需求一的结果数据
   *
   * @param sc
   * @param taskID
   * @param totalSessionCount
   * @param totalSessionLength
   * @param invalidSessionCount
   * @param preSessionLengthLevelSessionCount
   * @param preDayAndHourOfSessionCount
   * @param preDayAndHourOfSessionLength
   * @param preDayAndHourOfInvalidSessionCount
   */
  def saveSessionAggrResult(sc: SparkContext, taskID: Long,
                            totalSessionCount: Long,
                            totalSessionLength: Double,
                            invalidSessionCount: Long,
                            preSessionLengthLevelSessionCount: Array[(String, Int)],
                            preDayAndHourOfSessionCount: Array[((String, Int), Int)],
                            preDayAndHourOfSessionLength: Array[((String, Int), Long)],
                            preDayAndHourOfInvalidSessionCount: Array[((String, Int), Int)]
                             ): Unit = {
    // 1. 将数据转换为字符串
    val json: String = JSONUtil.mergeSessionAggrResultToJSONString(totalSessionCount, totalSessionLength, invalidSessionCount, preSessionLengthLevelSessionCount, preDayAndHourOfSessionCount, preDayAndHourOfSessionLength, preDayAndHourOfInvalidSessionCount)

    // TODO: 自己进行具体的存储
    /**
     * 可以直接使用jdbc的方法保存数据，或者可以使用JDBCHelp
     */
    println(json)
  }



  def getActionRDDByFilter(sqlContext: SQLContext, taskParam: JSONObject): RDD[UserVisitSessionRecord] = {
    // 1. 获取过滤参数
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    // 职业的过滤参数是一个多选的值，中间使用","进行分割
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    /**
     * professionals :  工人，老师，白领
     * where professionals in (工人,老师,白领)
     */

    // 2. 获取是否需要join其它表信息
    val needJoinUserInfo = if (sex.isDefined || professionals.isDefined) Some(true) else None

    // 3. 编写hql语句
    val hql =
      s"""
         |SELECT
         |  uva.*
         |FROM
         | user_visit_action uva
         | ${needJoinUserInfo.map(v => " JOIN user_info ui on uva.user_id = ui.user_id ").getOrElse("")}
          |WHERE 1 = 1
    | ${startDate.map(date => s" AND uva.date >= '${date}' ").getOrElse("")}
    | ${endDate.map(date => s" AND uva.date <= '${date}' ").getOrElse("")}
    | ${sex.map(v => s" AND ui.sex = '${v}' ").getOrElse("")}
    | ${professionals.map(v => s" AND ui.professional IN ${v.split(",").map(_.trim).filter(_.nonEmpty).map(t => s"'${v}'").mkString("(", ",", ")")} ").getOrElse("")}
    """.stripMargin
    println(s"\n===================================\n${hql}\n===================================\n");

    // 4. hql语句执行
    //df是所有生成的数据中过滤（根据j2e用户传参数到数据库中的判断）之后的信息
    val df = sqlContext.sql(hql)

    // 5. DataFrame转换为RDD
    val columnNames = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")
    df.map(row => {
      val date: String = row.getAs[String](columnNames(0))
      val userId: Long = row.getAs[Long](columnNames(1))
      val sessionId: String = row.getAs[String](columnNames(2))
      val pageId: Long = row.getAs[Long](columnNames(3))
      val actionTime: String = row.getAs[String](columnNames(4))
      val searchKeyword: String = row.getAs[String](columnNames(5))
      val clickCategoryId: String = row.getAs[String](columnNames(6))
      val clickProductId: String = row.getAs[String](columnNames(7))
      val orderCategoryIds: String = row.getAs[String](columnNames(8))
      val orderProductIds: String = row.getAs[String](columnNames(9))
      val payCategoryIds: String = row.getAs[String](columnNames(10))
      val payProductIds: String = row.getAs[String](columnNames(11))
      val cityId: Int = row.getAs[Int](columnNames(12))

      new UserVisitSessionRecord(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds, cityId)
    })
  }
}
