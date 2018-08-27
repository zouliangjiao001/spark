package com.ibeifeng.senior.usertrack.spark.product

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.dao.factory.DAOFactory
import com.ibeifeng.senior.usertrack.domain.Task
import com.ibeifeng.senior.usertrack.mock.MockDataUtils
import com.ibeifeng.senior.usertrack.spark.util.{SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.ParamUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by ibf on 2017/11/15.
 */
object AreaTop10ProductSpark {
  lazy val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val username = ConfigurationManager.getProperty(Constants.JDBC_USER)
  lazy val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  lazy val props = {
    val p = new Properties()
    p.put("user", username)
    p.put("password", password)
    p
  }
  def main(args: Array[String]) {
    //一、前期处理
    //1.1根据传入的taskID获取对应任务的过滤参数
    //a、获取taskid
    val taskID = ParamUtils.getTaskIdFromArgs(args)
    //b、从数据库中获取任务对象
    val task: Task = if (taskID == null) {
      //参数异常
      throw new IllegalArgumentException("参数异常")
    } else {
      //读取数据库的数据
      /**
       * 读取RDBMs的数据，可以通过以下的几种方式：
       * 使用第三方框架：Mybatis，hibernate（封装了JDBC）
       * 编写原始的jdbc来读取数据（dao模式）
       */
      //获取DAO对象
      val taskDao = DAOFactory.getTaskDAO
      //获取task
      taskDao.findByTaskId(taskID)
    }
    //c.task对象获取过滤参数
    val taskParam = ParamUtils.getTaskParam(task)
    if (taskParam == null || taskParam.isEmpty) {
      throw new
          IllegalArgumentException("获取任务过滤参数失败")
    }


    //上下文构建
    //2.1获取相关参数
    val appName = Constants.SPARK_APP_NAME_SESSION + taskID
    //判断是否是本地运行
    val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    //2.2创建上下文
    val conf = SparkConfUtil.generateSparkConf(appName, isLocal)
    //2.3创建SC
    val sc = SparkContextUtil.getSparkContext(conf)
    //2.4创建sqlContext对象，专门用于数据读取操作
    val sqlContext = SQLContextUtil.getInstance(
      sc,
      integratedHive = isLocal,
      generateMockData = (sc, sqlContext) => {
        //产生模拟数据，当generateMockData给定的时候就要执行模拟数据的生产
        if (isLocal) {
          //产生模拟数据
          MockDataUtils.mockData(sc, sqlContext)
          MockDataUtils.loadProductInfoMockData(sc, sqlContext)
        }
      }
    )
    def isEmpty(str: String) = str == null || str.trim.isEmpty
    sqlContext.udf.register("isNotEmpty", (str: String) => !isEmpty(str))
    //101:上海:1,102:杭州:2
    sqlContext.udf.register("concat_int_string", (id: Int, name: String) => s"${id}:${name}:1")
    sqlContext.udf.register("get_string_from_json", (value: String, field: String) => {
      JSON.parseObject(value).getString(field)
    })
    sqlContext.udf.register("group_contact", GroupConcatUDAF)
    //三、具体业务代码
    //3.1根据过滤参数读取数据
    val actionDataFrame = getActionDataFrameByFilter(sqlContext, taskParam)
    //3.2读取city_info的信息
    val cityInfoDataFrame = getCityInfoDataFrame(sqlContext)
    //3.3将用户行为数据和city_info数据进行join，并且注册成为临时表
    generateTempProductBasicTable(sqlContext, actionDataFrame, cityInfoDataFrame)
    //3.4计算各个区域的各个商品的点击次数，并将其注册成为临时表
    generateTempAreaProductCountTable(sqlContext)
    //3.5获取各个区域点击次数最多的Top10商品id，并将其注册成为临时表
    generateTempAreaTop10ProductCountTable(sqlContext)
    //3.6补全商品信息字段，并将其注册成为临时表
    generateTempAreaTop10FullProductCountTable(sqlContext)
    //3.7将最终结果保存到关系型数据库上，或者HDFS上
    persistAreaTop10FullProductCountTable(sqlContext)


  }

    /**
     * 将最终结果保存到关系型数据库/HDFS上
     *
     * @param sqlContext
     */
    def persistAreaTop10FullProductCountTable(sqlContext: SQLContext): Unit = {
      /**
       * 数据需要输出到关系型数据中===>DataFrame怎么讲数据输出到关系型数据库中
       * 一、通过DataFrame的write.jdbc进行数据输出操作
       * write输出的四中策略:
       * append: 追加
       * overwrite：覆盖
       * ingore：表存在/数据存在，就不进行数据输出操作
       * error：表存在/数据存在，就直接报错
       * 注意：在数据输出到HDFS上的时候，检测的是输出文件夹是否存在；当输出到RDBMs的时候，检查的是表是否存在
       * 如果洗完实现Insert Or Update操作的话，通过write.jdbc是无法实现的===>实际工作中直接使用write.jdbc将数据写入到RDBMs中的比较少
       * 二、可以直接调用DataFrame的foreachPartition API自定义数据输出代码
       * 三、可以通过将DataFrame转换为RDD后进行RDD的数据输出
       */
      // 1. 读取数据形成DataFrame
      val df = sqlContext.table("tmp_area_top10_full_product_count")
      // 2. TODO: 作业，将df的结果输出到表tb_area_top10_product, 要求进行insert or update操作
      // 2.2 额外，进行一下数据展示
      df.show(100)
    }

    /**
     * 补全商品信息字段信息，并将结果注册为临时表:tmp_area_top10_full_product_count
     *
     * @param sqlContext
     */
    def generateTempAreaTop10FullProductCountTable(sqlContext: SQLContext): Unit = {
      /** *
        * 获取json中的自营商品或者第三方商品的信息
        * 使用if语句块
        * if(condition, true-value, false-value)
        */
      /**
       * area: 华东、华南、华北、西南、西北、东北、华中
       * 地域级别的划分：
       * 华东、华南、华北 ===> A
       * 华中、东北 ===> B
       * 西南 ===> C
       * 西北 ===> D
       * 其它 ===> E
       */
      // 1. 构建HQL
      val hql =
        """
          | SELECT
          |   CASE
          |     WHEN tatpc.area = '华东' OR tatpc.area = '华南' OR tatpc.area = '华北' THEN 'A'
          |     WHEN tatpc.area = '华中' OR tatpc.area = '东北' THEN 'B'
          |     WHEN tatpc.area = '西南' THEN 'C'
          |     WHEN tatpc.area = '西北' THEN 'D'
          |     ELSE 'E'
          |   END as area_level,
          |   tatpc.cpi as product_id,
          |   tatpc.click_count,
          |   tatpc.city_infos,
          |   tatpc.area,
          |   pi.product_name,
          |   if (get_string_from_json(pi.extend_info, 'product_type') == '0', '自营商品', if(get_string_from_json(pi.extend_info, 'product_type') == '1', '第三方商品', '未知')) AS product_type
          | FROM tmp_area_top10_product_count tatpc
          |   LEFT JOIN product_info pi ON tatpc.cpi = pi.product_id
        """.stripMargin

      // 2. 执行hql
      val df = sqlContext.sql(hql)

      // 3. 结果保存临时表
      df.registerTempTable("tmp_area_top10_full_product_count")
    }

    /**
     * 获取各个区域点击次数最多的Top10商品id,并将其注册为临时表:tmp_area_top10_product_count
     *
     * @param sqlContext
     */
    def generateTempAreaTop10ProductCountTable(sqlContext: SQLContext): Unit = {
      // 1. 构建hql语句
      val hql =
        """
          | SELECT
          |  tmp.area, tmp.cpi, tmp.click_count, tmp.city_infos
          | FROM (
          |   SELECT
          |     area, cpi, click_count, city_infos,
          |     ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) AS rnk
          |   FROM
          |     tmp_area_product_count
          | ) AS tmp
          | WHERE tmp.rnk <= 10
        """.stripMargin

      // 2. 执行hql语句
      val df = sqlContext.sql(hql)

      // 3. 注册成为临时表
      df.registerTempTable("tmp_area_top10_product_count")
    }

    /**
     * 计算各个区域的各个商品的点击次数，并将其注册成为临时表:tmp_area_product_count
     * 一条数据就是一个点击记录，不需要涉及到去重操作
     *
     * @param sqlContext
     */
    def generateTempAreaProductCountTable(sqlContext: SQLContext): Unit = {
      /**
       * 在统计各个区域、各个商品的点击数量的时候，能不能不要将城市id和城市名称丢掉
       * eg:
       * 华东 101 上海 1
       * 华东 102 杭州 1
       * 华东 103 南京 1
       * 华北 201 北京 2
       * ......
       *
       * 期望结果:
       * 华东 1 3 101:上海:1,102:杭州:1,103:南京:1
       * 华北 2 1 201:北京:1
       * .....
       *
       * SparkSQL自定义函数:
       * UDF: 普通函数，一条输入，一条输出
       * UDAF：聚合类函数，多条输出，一条输出，用于group by中
       *
       * 步骤：
       * 1. 将id和名称连接形成一个字符串 ==> UDF
       * 2. 将连接之后的字符串按照group组进行合并操作，形成一个字符串 ===> UDAF
       */
      // 1. 编写hql语句
      val hql =
        """
          |SELECT
          |   area, cpi, COUNT(1) AS click_count,
          |   group_contact(concat_int_string(city_id, city_name)) AS city_infos
          |FROM tmp_product_basic
          |GROUP BY area, cpi
        """.stripMargin
      // 2. 执行hql得到结果
      val df = sqlContext.sql(hql)
      // 3. 结果DataFrame注册成为临时表
      df.registerTempTable("tmp_area_product_count")
    }

    /**
     * 合并两个dataframe的数据(根据ci和city_id来进行合并)，并将其注册成为临时表:tmp_product_basic
     *
     * @param sqlContext
     * @param action
     * @param cityInfo
     */
    def generateTempProductBasicTable(sqlContext: SQLContext, action: DataFrame, cityInfo: DataFrame): Unit = {
      // 1. 引入映射转换
      import sqlContext.implicits._
      // 2. 进行数据join并获取最终的字段信息
      val joinResultDataFrame = action
        .join(cityInfo, $"ci" === $"city_id")
        .select("city_id", "city_name", "area", "cpi")
      // 3. 将DataFrame注册成为临时表
      joinResultDataFrame.registerTempTable("tmp_product_basic")
    }

    /**
     * 从RDBMs中读取城市信息数据
     * 两种方式：
     * 1. 通过自定义InputFormat/MapReduce自带的DBInputFormat进行数据读取形成RDD，然后转换为DataFrame
     * 2. 通过SparkSQL的read.jdbc读取数据直接形成DataFrame
     *
     * @param sqlContext
     * @return
     */
    def getCityInfoDataFrame(sqlContext: SQLContext): DataFrame = {
      // 1. 给定表名称
      val table = "city_info"
      // 2. 构建DataFrame并返回
      val df = sqlContext.read.jdbc(url, table, props)

      // 返回结果
      df
    }

    /**
     * 根据任务过滤参数进行数据过滤，并返回一个DataFrame
     *
     * @param sqlContext
     * @param taskParam
     * @return
     */
    def getActionDataFrameByFilter(sqlContext: SQLContext, taskParam: JSONObject): DataFrame = {
      // 1. 获取过滤参数
      val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
      val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
      val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
      // 职业的过滤参数是一个多选的值，中间使用","进行分割
      val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)

      professionals.map(v => {
        val str: String = v.split(",").map(_.trim).filter(_.nonEmpty).map(t => s"'${v}'").mkString("(", ",", ")")
        s" ui.professional IN ${str} "
      })

      // 2. 获取是否需要join其它表信息
      val needJoinUserInfo = if (sex.isDefined || professionals.isDefined) Some(true) else None

      // 3. 编写hql语句
      val hql =
        s"""
           |SELECT
           |  uva.click_product_id AS cpi, uva.city_id AS ci
           |FROM
           | user_visit_action uva
           | ${needJoinUserInfo.map(v => " JOIN user_info ui on uva.user_id = ui.user_id ").getOrElse("")}
            |WHERE isNotEmpty(uva.click_product_id)
            | ${startDate.map(date => s" AND uva.date >= '${date}' ").getOrElse("")}
            | ${endDate.map(date => s" AND uva.date <= '${date}' ").getOrElse("")}
            | ${sex.map(v => s" AND ui.sex = '${v}' ").getOrElse("")}
            | ${professionals.map(v => s" AND ui.professional IN ${v.split(",").map(_.trim).filter(_.nonEmpty).map(t => s"'${v}'").mkString("(", ",", ")")} ").getOrElse("")}
    """.stripMargin
      println(s"\n===================================\n${hql}\n===================================\n");

      // 4. hql语句执行
      val df = sqlContext.sql(hql)

      // 5. 返回结果
      df
    }
  }
