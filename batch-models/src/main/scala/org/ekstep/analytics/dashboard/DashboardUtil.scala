package org.ekstep.analytics.dashboard

import org.apache.hadoop.fs.{FileSystem, Path}
import redis.clients.jedis.Jedis
import org.apache.spark.SparkContext
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone, format}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.params.ScanParams

import java.io.{File, Serializable}
import java.util
import scala.util.Try
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class DummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class DummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class DashboardConfig (
    debug: String,
    validation: String,
    // kafka connection config
    broker: String,
    compression: String,
    // redis connection config
    redisHost: String,
    redisPort: Int,
    redisDB: Int,
    // for blob storage
    store: String,
    container: String,
    key: String,
    secret: String,
    // other hosts connection config
    sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
    sparkElasticsearchConnectionHost: String, fracBackendHost: String,
    sparkMongoConnectionHost: String,
    // kafka topics
    roleUserCountTopic: String, orgRoleUserCountTopic: String,
    allCourseTopic: String, userCourseProgramProgressTopic: String,
    fracCompetencyTopic: String, courseCompetencyTopic: String, expectedCompetencyTopic: String,
    declaredCompetencyTopic: String, competencyGapTopic: String, userOrgTopic: String, orgTopic: String,
    userAssessmentTopic: String, assessmentTopic: String,
    // cassandra key spaces
    cassandraUserKeyspace: String,
    cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String,
    cassandraUserFeedKeyspace: String,
    // cassandra table details
    cassandraUserTable: String, cassandraUserRolesTable: String, cassandraOrgTable: String,
    cassandraUserEnrolmentsTable: String, cassandraContentHierarchyTable: String,
    cassandraRatingSummaryTable: String, cassandraUserAssessmentTable: String,
    cassandraRatingsTable: String, cassandraOrgHierarchyTable: String,
    cassandraUserFeedTable: String,
    cassandraCourseBatchTable: String, cassandraLearnerStatsTable: String,

    // redis keys
    redisRegisteredOfficerCountKey: String, redisTotalOfficerCountKey: String, redisOrgNameKey: String,
    redisTotalRegisteredOfficerCountKey: String, redisTotalOrgCountKey: String,
    redisExpectedUserCompetencyCount: String, redisDeclaredUserCompetencyCount: String,
    redisUserCompetencyDeclarationRate: String, redisOrgCompetencyDeclarationRate: String,
    redisUserCompetencyGapCount: String, redisUserCourseEnrolmentCount: String,
    redisUserCompetencyGapEnrolmentRate: String, redisOrgCompetencyGapEnrolmentRate: String,
    redisUserCourseCompletionCount: String, redisUserCompetencyGapClosedCount: String,
    redisUserCompetencyGapClosedRate: String, redisOrgCompetencyGapClosedRate: String,

    // mongoDB configurations
    mongoDBCollection: String,
    mongoDatabase: String,
    platformRatingSurveyId: String,

    // for reports
    mdoIDs: String,
    standaloneAssessmentReportPath: String,
    userReportPath: String,
    userEnrolmentReportPath: String,
    courseReportPath: String,
    taggedUsersPath: String,
    cbaReportPath: String,
    blendedReportPath: String,

    // for weekly claps
    cutoffTime: Float
 ) extends Serializable


object DashboardUtil extends Serializable {

  implicit var debug: Boolean = false
  implicit var validation: Boolean = false

  val allowedContentCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment")

  def validateContentCategories(categories: Seq[String]): Unit = {

  }

  object Test extends Serializable {
    /**
     * ONLY FOR TESTING!!, do not use to create spark context in model or job
     * */
    def getSessionAndContext(name: String, config: Map[String, AnyRef]): (SparkSession, SparkContext, FrameworkContext) = {
      val cassandraHost = config.getOrElse("sparkCassandraConnectionHost", "localhost").asInstanceOf[String]
      val mongodbHost = config.getOrElse("sparkMongoConnectionHost", "localhost").asInstanceOf[String]
      val esHost = config.getOrElse("sparkElasticsearchConnectionHost", "localhost").asInstanceOf[String]
      val spark: SparkSession =
        SparkSession
          .builder()
          .appName(name)
          .config("spark.master", "local[*]")
          .config("spark.cassandra.connection.host", cassandraHost)
          .config("spark.cassandra.output.batch.size.rows", "10000")
          //.config("spark.cassandra.read.timeoutMS", "60000")
          .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
          .config("spark.sql.caseSensitive", "true")
          .config("es.nodes", esHost)
          .config("es.port", "9200")
          .config("es.index.auto.create", "false")
          .config("es.nodes.wan.only", "true")
          .config("es.nodes.discovery", "false")
          .config("spark.mongodb.input.uri", s"mongodb://${mongodbHost}/Nodebb.Objects")
          .config("spark.mongodb.input.sampleSize", 50000)
          .getOrCreate()
      val sc: SparkContext = spark.sparkContext
      val fc: FrameworkContext = new FrameworkContext()
      sc.setLogLevel("WARN")
      (spark, sc, fc)
    }

    def time[R](block: => R): (Long, R) = {
      val t0 = System.currentTimeMillis()
      val result = block // call-by-name
      val t1 = System.currentTimeMillis()
      ((t1 - t0), result)
    }

  }

  object StorageUtil extends Serializable {

    def getStorageService(config: DashboardConfig): BaseStorageService = {
      val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
      val storageType = "s3"
      val storageKey = AppConf.getConfig(config.key)
      val storageSecret = AppConf.getConfig(config.secret)

      val storageService = if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
        new CustomS3StorageService(
          StorageConfig(storageType, storageKey, storageSecret, Option(storageEndpoint))
        )
      } else {
        StorageServiceFactory.getStorageService(
          StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"))
        )
      }
      storageService
    }


    def removeFile(path: String)(implicit spark: SparkSession): Unit = {
      val successFile = new Path(path)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(successFile)) {
        fs.delete(successFile, true)
      }

    }

    def renameCSV(ids: util.List[String], reportTempPath: String, fileName: String): Unit = {
      ids.foreach(id => {
        val orgReportPath = new File(s"${reportTempPath}/mdoid=${id}/")
        val csvFiles = orgReportPath.listFiles().filter(f => {f.getName.startsWith("part-") && f.getName.endsWith(".csv")})

        csvFiles.zipWithIndex.foreach(csvFileWithIndex => {
          val (csvFile, index) = csvFileWithIndex
          val customizedPath = new File(s"${reportTempPath}/mdoid=${id}/${fileName}${if (index == 0) "" else index}.csv")
          // println(s"RENAME: renaming ${csvFile} to ${customizedPath}")
          csvFile.renameTo(customizedPath)
        })

      })
    }

  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getThisWeekDates():(String, String, String, String) = {

    val istTimeZone = DateTimeZone.forID("Asia/Kolkata")
    val currentDate = DateTime.now(istTimeZone)
    val dateFormatter: format.DateTimeFormatter = format.DateTimeFormat.forPattern("yyyy-MM-dd").withZone(istTimeZone)
    val formatter: format.DateTimeFormatter = format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(istTimeZone)

    val today = dateFormatter.print(currentDate)
    val yesterday = dateFormatter.print(currentDate.minusDays(1))
    val startOfWeek = currentDate.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay()
    val endOfWeek = startOfWeek.plusDays(6).withTime(23,59,59,999)
    (formatter.print(startOfWeek), formatter.print(endOfWeek), today, yesterday)
  }

  /* Util functions */
  def csvWrite(df: DataFrame, path: String, header: Boolean = true): Unit = {
    df.write.mode(SaveMode.Overwrite).format("csv").option("header", header.toString).save(path)
  }

  def csvWritePartition(df: DataFrame, path: String, partitionKey: String, header: Boolean = true): Unit = {
    df.write.mode(SaveMode.Overwrite).format("csv").option("header", header.toString)
      .partitionBy(partitionKey).save(path)
  }

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def kafkaDispatch(data: RDD[String], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic, "compression" -> conf.compression), data)
    }
  }

  def kafkaDispatch(data: DataFrame, topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def kafkaDispatchDS[T](data: Dataset[T], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  /* redis util functions */
  var redisConnect: Jedis = null
  var redisHost: String = ""
  var redisPort: Int = 0
  def closeRedisConnect(): Unit = {
    if (redisConnect != null) {
      redisConnect.close()
      redisConnect = null
    }
  }
  def redisUpdate(key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    redisUpdate(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def redisUpdate(db: Int, key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    redisUpdate(conf.redisHost, conf.redisPort, db, key, data)
  }
  def redisUpdate(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    try {
      redisUpdateWithoutRetry(host, port, db, key, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        redisUpdateWithoutRetry(host, port, db, key, data)
    }
  }
  def redisUpdateWithoutRetry(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    var cleanedData = ""
    if (data == null || data.isEmpty) {
      println(s"WARNING: data is empty, setting data='' for redis key=${key}")
      cleanedData = ""
    } else {
      cleanedData = data
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }

    if (jedis.getDB != db) jedis.select(db)
    jedis.set(key, cleanedData)
  }
  def redisDispatch(key: String, data: util.Map[String, String])(implicit conf: DashboardConfig): Unit = {
    redisDispatch(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def redisDispatch(db: Int, key: String, data: util.Map[String, String])(implicit conf: DashboardConfig): Unit = {
    redisDispatch(conf.redisHost, conf.redisPort, db, key, data)
  }
  def redisDispatch(host: String, port: Int, db: Int, key: String, data: util.Map[String, String]): Unit = {
    try {
      redisDispatchWithoutRetry(host, port, db, key, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        redisDispatchWithoutRetry(host, port, db, key, data)
    }
  }
  def redisDispatchWithoutRetry(host: String, port: Int, db: Int, key: String, data: util.Map[String, String]): Unit = {
    if (data == null || data.isEmpty) {
      println(s"WARNING: map is empty, skipping saving to redis key=${key}")
      return
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    redisReplaceMap(jedis, key, data)
  }

  def redisGetHashKeys(jedis: Jedis, key: String): Seq[String] = {
    val keys = ListBuffer[String]()
    val scanParams = new ScanParams().count(100)
    var cur = ScanParams.SCAN_POINTER_START
    do {
      val scanResult = jedis.hscan(key, cur, scanParams)
      scanResult.getResult.foreach(res => {
        keys += res.getKey
      })
      cur = scanResult.getCursor
    } while (!cur.equals(ScanParams.SCAN_POINTER_START))
    keys.toList
  }
  def redisReplaceMap(jedis: Jedis, key: String, data: util.Map[String, String]): Unit = {
    // this deletes the keys that do not exist anymore manually
    val existingKeys = redisGetHashKeys(jedis, key)
    val toDelete = existingKeys.toSet.diff(data.keySet())
    if (toDelete.nonEmpty) jedis.hdel(key, toDelete.toArray:_*)
    // this will update redis hash map keys and create new ones, but will not delete ones that have been deleted
    jedis.hmset(key, data)
  }
  def getOrCreateRedisConnect(host: String, port: Int): Jedis = {
    if (redisConnect == null) {
      redisConnect = createRedisConnect(host, port)
    } else if (redisHost != host || redisPort != port) {
      redisConnect = createRedisConnect(host, port)
    }
    redisConnect
  }
  def getOrCreateRedisConnect(conf: DashboardConfig): Jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
  def createRedisConnect(host: String, port: Int): Jedis = {
    redisHost = host
    redisPort = port
    if (host == "") return null
    new Jedis(host, port, 30000)
  }
  def createRedisConnect(conf: DashboardConfig): Jedis = createRedisConnect(conf.redisHost, conf.redisPort)
  /* redis util functions over */

  /**
   * Convert data frame into a map, and save to redis
   *
   * @param redisKey key to save df data to
   * @param df data frame
   * @param keyField column name that forms the key (must be a string)
   * @param valueField column name that forms the value
   * @tparam T type of the value column
   */
  def redisDispatchDataFrame[T](redisKey: String, df: DataFrame, keyField: String, valueField: String)(implicit conf: DashboardConfig): Unit = {
    redisDispatch(redisKey, dfToMap[T](df, keyField, valueField))
  }

  def apiThrowException(method: String, url: String, body: String): String = {
    val request = method.toLowerCase() match {
      case "post" => new HttpPost(url)
      case _ => throw new Exception(s"HTTP method '${method}' not supported")
    }
    request.setHeader("Content-type", "application/json")  // set the Content-type
    request.setEntity(new StringEntity(body))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(request)  // send the request
    val statusCode = response.getStatusLine.getStatusCode  // get status code
    if (statusCode < 200 || statusCode > 299) {
      throw new Exception(s"ERROR: got status code=${statusCode}, response=${EntityUtils.toString(response.getEntity)}")
    } else {
      EntityUtils.toString(response.getEntity)
    }
  }

  def api(method: String, url: String, body: String): String = {
    try {
      apiThrowException(method, url, body)
    } catch {
      case e: Throwable => {
        println(s"ERROR: ${e.toString}")
        return ""
      }
    }
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def durationFormat(df: DataFrame, inCol: String, outCol: String = null): DataFrame = {
    val outColName = if (outCol == null) inCol else outCol
    df.withColumn(outColName,
      format_string("%02d:%02d:%02d",
        expr(s"${inCol} / 3600").cast("int"),
        expr(s"${inCol} % 3600 / 60").cast("int"),
        expr(s"${inCol} % 60").cast("int")
      )
    )
  }

  def dataFrameFromJSONString(jsonString: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataset = spark.createDataset(jsonString :: Nil)
    spark.read.option("mode", "DROPMALFORMED").option("multiline", value = true).json(dataset)
  }
  def emptySchemaDataFrame(schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
    // TODO: tech-debt, use proper spark druid connector when available, no official connector for this version of spark as of now
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":false,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    api("POST", url, requestBody)
  }

  def druidDFOption(query: String, host: String, resultFormat: String = "object", limit: Int = 10000)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = druidSQLAPI(query, host, resultFormat, limit)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: druidSQLAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result).persist(StorageLevel.MEMORY_ONLY)
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `error` field in the json
    if (hasColumn(df, "error")) {
      println(s"ERROR: druidSQLAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  def dfToMap[T](df: DataFrame, keyField: String, valueField: String): util.Map[String, String] = {
    // previous in-efficient implementation
    // val map = new util.HashMap[String, String]()
    // df.collect().foreach(row => map.put(row.getAs[String](keyField), row.getAs[T](valueField).toString))
    // map

    // faster implementation
    df.rdd.map(row => (row.getAs[String](keyField), row.getAs[T](valueField).toString))
      .collectAsMap()
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra")
      .option("inferSchema", "true")
      .option("keyspace", keySpace)
      .option("table", table)
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def elasticSearchDataFrame(host: String, index: String, query: String, fields: Seq[String], arrayFields: Seq[String] = Seq())(implicit spark: SparkSession): DataFrame = {
    var dfr = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", "false")
      .option("es.nodes", host)
      .option("es.port", "9200")
      .option("es.index.auto.create", "false")
      .option("es.nodes.wan.only", "true")
      .option("es.nodes.discovery", "false")
    if (arrayFields.nonEmpty) {
      dfr = dfr.option("es.read.field.as.array.include", arrayFields.mkString(","))
    }
    var df = dfr.option("query", query).load(index)
    df = df.select(fields.map(f => col(f)):_*).persist(StorageLevel.MEMORY_ONLY) // select only the fields we need and persist
    df
  }

  def mongodbTableAsDataFrame(mongodatabase: String, collection: String)(implicit spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("topiccount", IntegerType, true)
      .add("postcount", IntegerType, true)
      .add("sunbird-oidcId", StringType, true)
      .add("username", StringType, true)
    val df = spark.read.schema(schema).format("com.mongodb.spark.sql.DefaultSource").option("database", mongodatabase).option("collection", collection).load()
    val filterDf = df.select("sunbird-oidcId").where(col("username").isNotNull or col("topiccount") > 0 and (col("postcount") > 0))
    val renamedDF = filterDf.withColumnRenamed("sunbird-oidcId", "userid")
    renamedDF
  }

  def writeToCassandra(data: DataFrame, keyspace: String, table: String)(implicit spark: SparkSession): Unit = {
    data.write.format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save()
  }

  /* Config functions */
  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String, default: String = ""): String = getConfig[String](config, key, default)
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideBrokerCompression(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.compression", "snappy")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")

  def parseConfig(config: Map[String, AnyRef]): DashboardConfig = {
    DashboardConfig(
      debug = getConfigModelParam(config, "debug"),
      validation = getConfigModelParam(config, "validation"),
      // kafka connection config
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      // redis connection config
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      //for blob storage
      store = getConfigModelParam(config, "store"),
      container = getConfigModelParam(config, "container"),
      key = getConfigModelParam(config, "key"),
      secret = getConfigModelParam(config, "secret"),
        // other hosts connection config
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      sparkMongoConnectionHost =  getConfigModelParam(config, "sparkMongoConnectionHost"),
      fracBackendHost = getConfigModelParam(config, "fracBackendHost"),
      // kafka topics
      roleUserCountTopic = getConfigSideTopic(config, "roleUserCount"),
      orgRoleUserCountTopic = getConfigSideTopic(config, "orgRoleUserCount"),
      allCourseTopic = getConfigSideTopic(config, "allCourses"),
      userCourseProgramProgressTopic = getConfigSideTopic(config, "userCourseProgramProgress"),
      fracCompetencyTopic = getConfigSideTopic(config, "fracCompetency"),
      courseCompetencyTopic = getConfigSideTopic(config, "courseCompetency"),
      expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetency"),
      declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetency"),
      competencyGapTopic = getConfigSideTopic(config, "competencyGap"),
      userOrgTopic = getConfigSideTopic(config, "userOrg"),
      orgTopic = getConfigSideTopic(config, "org"),
      userAssessmentTopic = getConfigSideTopic(config, "userAssessment"),
      assessmentTopic = getConfigSideTopic(config, "assessment"),
      // cassandra key spaces
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraCourseKeyspace = getConfigModelParam(config, "cassandraCourseKeyspace"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      cassandraUserFeedKeyspace = getConfigModelParam(config, "cassandraUserFeedKeyspace"),
      // cassandra table details
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraUserRolesTable = getConfigModelParam(config, "cassandraUserRolesTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      cassandraUserEnrolmentsTable = getConfigModelParam(config, "cassandraUserEnrolmentsTable"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable"),
      cassandraUserAssessmentTable = getConfigModelParam(config, "cassandraUserAssessmentTable"),
      cassandraRatingsTable = getConfigModelParam(config, "cassandraRatingsTable"),
      cassandraOrgHierarchyTable = getConfigModelParam(config, "cassandraOrgHierarchyTable"),
      cassandraUserFeedTable = getConfigModelParam(config, "cassandraUserFeedTable"),
      cassandraCourseBatchTable = getConfigModelParam(config, "cassandraCourseBatchTable"),
      cassandraLearnerStatsTable = getConfigModelParam(config, "cassandraLearnerStatsTable"),
      // redis keys
      redisRegisteredOfficerCountKey = "mdo_registered_officer_count",
      redisTotalOfficerCountKey = "mdo_total_officer_count",
      redisOrgNameKey = "mdo_name_by_org",
      redisTotalRegisteredOfficerCountKey = "mdo_total_registered_officer_count",
      redisTotalOrgCountKey = "mdo_total_org_count",
      redisExpectedUserCompetencyCount = "dashboard_expected_user_competency_count",
      redisDeclaredUserCompetencyCount = "dashboard_declared_user_competency_count",
      redisUserCompetencyDeclarationRate = "dashboard_user_competency_declaration_rate",
      redisOrgCompetencyDeclarationRate = "dashboard_org_competency_declaration_rate",
      redisUserCompetencyGapCount = "dashboard_user_competency_gap_count",
      redisUserCourseEnrolmentCount = "dashboard_user_course_enrollment_count",
      redisUserCompetencyGapEnrolmentRate = "dashboard_user_competency_gap_enrollment_rate",
      redisOrgCompetencyGapEnrolmentRate = "dashboard_org_competency_gap_enrollment_rate",
      redisUserCourseCompletionCount = "dashboard_user_course_completion_count",
      redisUserCompetencyGapClosedCount = "dashboard_user_competency_gap_closed_count",
      redisUserCompetencyGapClosedRate = "dashboard_user_competency_gap_closed_rate",
      redisOrgCompetencyGapClosedRate = "dashboard_org_competency_gap_closed_rate",

      //mongoBD configurations
      mongoDBCollection =  getConfigModelParam(config, "mongoDBCollection"),
      mongoDatabase = getConfigModelParam(config, "mongoDatabase"),
      platformRatingSurveyId = getConfigModelParam(config, "platformRatingSurveyId"),

      // for reports
      mdoIDs = getConfigModelParam(config, "mdoIDs"),
      standaloneAssessmentReportPath = getConfigModelParam(config, "standaloneAssessmentReportPath"),
      userReportPath = getConfigModelParam(config, "userReportPath"),
      userEnrolmentReportPath = getConfigModelParam(config, "userEnrolmentReportPath"),
      courseReportPath = getConfigModelParam(config, "courseReportPath"),
      taggedUsersPath = getConfigModelParam(config, "taggedUsersPath"),
      cbaReportPath = getConfigModelParam(config, "cbaReportPath"),
      blendedReportPath = getConfigModelParam(config, "blendedReportPath"),

      // for weekly claps
      cutoffTime = getConfigModelParam(config, "cutoffTime").toFloat

    )
  }
  /* Config functions end */

  def checkAvailableColumns(df: DataFrame, expectedColumnsInput: List[String]) : DataFrame = {
    expectedColumnsInput.foldLeft(df) {
      (df, column) => {
        if(!df.columns.contains(column)) {
          df.withColumn(column, lit(null).cast(StringType))
        } else df
      }
    }
  }

  /**
   * return parsed int or zero if parsing fails
   * @param s string to parse
   * @return int or zero
   */
  def intOrZero(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  /***
   * Validate if return value from one code block is equal to the return value from other block. Uses blocks so that the
   * spark code in blocks is only executed if validation=true
   *
   * @param msg info on what is being validated
   * @tparam T return type from the blocks
   * @throws AssertionError if values from blocks do not match
   */
  @throws[AssertionError]
  def validate[T](block1: => T, block2: => T, msg: String = ""): Unit = {
    if (validation) {
      val r1 = block1
      val r2 = block2
      if (r1.equals(r2)) {
        println(s"VALIDATION PASSED: ${msg}")
        println(s"  - value = ${r1}")
      } else {
        println(s"VALIDATION FAILED: ${msg}")
        println(s"  - value ${r1} does not equal value ${r2}")
        // throw new AssertionError("Validation Failed")
      }
    }
  }

  def show(df: DataFrame, msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def showDS[T](ds: Dataset[T], msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      ds.show()
      println("Count: " + ds.count())
    }
    ds.printSchema()
  }
}
