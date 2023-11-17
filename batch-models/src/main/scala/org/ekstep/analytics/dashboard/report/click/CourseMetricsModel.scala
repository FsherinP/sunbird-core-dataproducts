package org.ekstep.analytics.dashboard.report.click

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}

import java.io.Serializable


object CourseMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.click.CourseMetricsModel"
  override def name() = "CourseMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyMetricsData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processCompetencyMetricsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // obtain and save user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, attach rating info, dispatch to kafka to be ingested by druid data-source: dashboards-courses
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "CuratedCollections","Curated Program"))

    val readLoc = "/home/analytics/click-stream-data/"
    val loc = "/home/analytics/click-stream-data/gen/"

    // object_id, object_type, clicks
    val clickDF = spark.read.format("csv").option("header", "true")
      .load(s"${readLoc}clicks-by-content-id.csv")
      .select(
        col("object_id").alias("courseID"),
        col("object_type").alias("category"),
        col("clicks")
      )
    show(clickDF, "clickDF")
    val clickWithDetailsDF = clickDF.join(allCourseProgramDetailsWithRatingDF, Seq("courseID", "category"), "left")
    show(clickWithDetailsDF, "clickWithDetailsDF")

    // click by id
    val clickByIDDF = clickWithDetailsDF.select("courseID", "courseName", "category", "clicks")
      .orderBy(desc("clicks"))
    show(clickByIDDF, "clickByIDDF")
    csvWrite(clickByIDDF.coalesce(1), s"${loc}clicks-by-content-name.csv")

    // click by provider
    val clickByProviderDF = clickWithDetailsDF.groupBy("courseOrgID", "courseOrgName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    show(clickByProviderDF, "clickByProviderDF")
    csvWrite(clickByProviderDF.coalesce(1), s"${loc}clicks-by-provider.csv")

    // click by course duration
    val clickByDuration = clickWithDetailsDF.withColumn("durationFloorHrs", expr("CAST(FLOOR(courseDuration / 3600) AS INTEGER)"))
      .groupBy("durationFloorHrs")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("durationFloorHrs")
    show(clickByDuration, "clickByDuration")
    csvWrite(clickByDuration.coalesce(1), s"${loc}clicks-by-duration.csv")

    // click by course rating
    val clickByRating = clickWithDetailsDF.withColumn("ratingFloor", expr("CAST(FLOOR(ratingAverage) AS INTEGER)"))
      .groupBy("ratingFloor")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("ratingFloor")
    show(clickByRating, "clickByRating")
    csvWrite(clickByRating.coalesce(1), s"${loc}clicks-by-rating.csv")

    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    val clickWithCompetencyDF = clickDF.join(allCourseProgramCompetencyDF, Seq("courseID", "category"), "left")

    // click by competencies
    val clickByCompetencyDF = clickWithCompetencyDF.groupBy("competencyID", "competencyName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    show(clickByCompetencyDF, "clickByCompetencyDF")
    csvWrite(clickByCompetencyDF.coalesce(1), s"${loc}clicks-by-comp.csv")

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")

    val contentUserStatusCountDF = allCourseProgramCompletionWithDetailsDF
      .where(expr("courseStatus IN ('Live', 'Retired')"))
      .groupBy("courseID", "courseName", "category")
      .agg(
        expr("COUNT(courseID)").alias("countEnrolled"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("countInProgress"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("countCompleted")
      )
    show(contentUserStatusCountDF, "contentUserStatusCountDF")
    val clickWithProgressCountsDF = clickDF.join(contentUserStatusCountDF, Seq("courseID", "category"), "left")
    show(clickWithProgressCountsDF, "clickWithProgressCountsDF")

    val clickByEnroll = bucketGroupBy(clickWithProgressCountsDF, "countEnrolled", "clicks")
    show(clickByEnroll, "clickByEnroll")
    csvWrite(clickByEnroll.coalesce(1),s"${loc}clicks-by-enroll.csv")

    val clickByInProgress = bucketGroupBy(clickWithProgressCountsDF, "countInProgress", "clicks")
    show(clickByInProgress, "clickByInProgress")
    csvWrite(clickByInProgress.coalesce(1), s"${loc}clicks-by-in-progress.csv")

    val clickByCompleted = bucketGroupBy(clickWithProgressCountsDF, "countCompleted", "clicks")
    show(clickByCompleted, "clickByCompleted")
    csvWrite(clickByCompleted.coalesce(1),s"${loc}clicks-by-completed.csv")

    closeRedisConnect()
  }

  def bucketGroupBy(df: DataFrame, bucketCol: String, groupCol: String): DataFrame = {
    val maxVal = df.agg(expr(s"MAX(${bucketCol})")).head().getLong(0)  // 12345
    val orderMaxVal = Math.log10(maxVal).floor.toLong  // 4
    val order = Math.pow(10, orderMaxVal - 1).toLong  // 1000

    df.withColumn(s"${bucketCol}Floor", expr(s"CAST((FLOOR(${bucketCol} / ${order}) * ${order}) AS LONG)"))
      .groupBy(s"${bucketCol}Floor")
      .agg(expr(s"SUM(${groupCol})").alias(s"${groupCol}"))
      .orderBy(s"${bucketCol}Floor")
  }

}