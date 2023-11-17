package org.ekstep.analytics.dashboard.report.assess_new

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable


object AllAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assess.AllAssessmentModel"
  override def name() = "AllAssessmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserAssessmentData(timestamp, config)
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
  def processUserAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    // obtain user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Standalone Assessment","Curated Program"), runValidation = false)

    var assessmentDF = allAssessmentESDataFrame(true)

    assessmentDF = addHierarchyColumn(assessmentDF, hierarchyDF, "cbpID", "data", children = true)
    show(assessmentDF, "assessmentDF 1")

    val cbpOrgDF = orgDF.select(
      col("orgID").alias("cbpOrgID"),
      col("orgName").alias("cbpOrgName"),
      col("orgStatus").alias("cbpOrgStatus")
    )
    assessmentDF = assessmentDF.join(cbpOrgDF, Seq("cbpOrgID"), "left")
    show(assessmentDF, "assessmentDF 2")

    assessmentDF = assessmentDF
      .select(
        col("cbpID"),
        col("cbpCategory"),
        col("cbpName"),
        col("cbpStatus"),
        col("cbpReviewStatus"),
        col("cbpDuration"),
        col("cbpChildCount"),
        col("cbpOrgID"),
        col("cbpOrgName"),
        col("cbpStatus"),

        col("data.children").alias("children"),
        col("data.publish_type").alias("cbpPublishType"),
        col("data.isExternal").cast(IntegerType).alias("cbpIsExternal"),
        col("data.contentType").alias("cbpContentType"),
        col("data.objectType").alias("cbpObjectType"),
        col("data.userConsent").alias("cbpUserConsent"),
        col("data.visibility").alias("cbpVisibility"),
        col("data.createdOn").alias("cbpCreatedOn"),
        col("data.lastUpdatedOn").alias("cbpLastUpdatedOn"),
        col("data.lastPublishedOn").alias("cbpLastPublishedOn"),
        col("data.lastSubmittedOn").alias("cbpLastSubmittedOn")
      )

    assessmentDF = timestampStringToLong(assessmentDF,
      Seq("cbpCreatedOn", "cbpLastUpdatedOn", "cbpLastPublishedOn", "cbpLastSubmittedOn"),
      "yyyy-MM-dd'T'HH:mm:ss")
    show(assessmentDF, "assessmentDF 3")

    val cbpChildrenDF = assessmentDF.select(
      col("cbpID"),
      explode_outer(col("children")).alias("ch")
    ).select(
      col("cbpID"),
      col("ch.identifier").alias("cbpChildID"),
      col("ch.name").alias("cbpChildName"),
      col("ch.duration").cast(FloatType).alias("cbpChildDuration"),
      col("ch.primaryCategory").alias("cbpChildPrimaryCategory"),
      col("ch.contentType").alias("cbpChildContentType"),
      col("ch.objectType").alias("cbpChildObjectType"),
      col("ch.showTimer").alias("cbpChildShowTimer"),
      col("ch.allowSkip").alias("cbpChildAllowSkip")
    )
    show(cbpChildrenDF, "cbpChildrenDF")

    var userAssessmentDF = userAssessmentDataFrame()

    var userAssessChildrenDF = userAssessmentDF.join(cbpChildrenDF,
      userAssessmentDF.col("assessChildID") === cbpChildrenDF.col("cbpChildID"), "inner")
      .drop("assessChildID")
    show(userAssessChildrenDF, "userAssessChildrenDF")

    val courseDF = allCourseProgramDetailsWithRatingDF
      .drop("count1Star", "count2Star", "count3Star", "count4Star", "count5Star")
    val userAssessChildrenDetailsDF = userAssessChildrenDF
      .join(assessmentDF.drop("children"), Seq("cbpID"), "left")
      .join(courseDF, Seq("courseID"), "left")
      .join(userOrgDF, Seq("userID"), "left")
    show(userAssessChildrenDetailsDF, "userAssessChildrenDetailsDF")

    var df = userAssessChildrenDetailsDF

    // get the mdoids for which the report are requesting
    // val mdoID = conf.mdoIDs
    // val mdoIDDF = mdoIDsDF(mdoID)

    // val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("assessOrgID"), col("orgName"))
    // df = df.join(mdoData, Seq("assessOrgID"), "inner")

    val latest = df.groupBy(col("cbpChildID"), col("userID"))
      .agg(max("assessEndTimestamp").alias("assessEndTimestamp"))

    df = df.join(latest, Seq("cbpChildID", "userID", "assessEndTimestamp"), "inner")
    show(df, "df.join(latest)")

    val caseExpression = "CASE WHEN assessPass == 1 AND assessUserStatus == 'SUBMITTED' THEN 'Pass' WHEN assessPass == 0 AND assessUserStatus == 'SUBMITTED' THEN 'Fail' " +
      " ELSE 'N/A' END"
    df = df.withColumn("Assessment_Status", expr(caseExpression))

    val caseExpressionCompletionStatus = "CASE WHEN assessUserStatus == 'SUBMITTED' THEN 'Completed' ELSE 'In progress' END"
    df = df.withColumn("Overall_Status", expr(caseExpressionCompletionStatus))

    df = df.withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))

    // val attemptCountDF = df.groupBy("userID", "assessID").agg(expr("COUNT(*)").alias("noOfAttempts"))

    df = df
      .dropDuplicates("userID", "cbpID")
      // .join(attemptCountDF, Seq("userID", "assessID"), "left")
      .select(
        col("userID"),
        col("userOrgID"),
        col("cbpCategory").alias("Type"),
        col("cbpID"),
        col("cbpOrgID"),
        col("fullName").alias("Full_Name"),
        col("cbpName").alias("CBP_Name"),
        col("Overall_Status"),
        col("Assessment_Status"),
        col("assessPassPercentage").alias("Percentage_Of_Score"),
        // col("noOfAttempts").alias("Number_of_Attempts"),
        col("maskedEmail").alias("Email"),
        col("maskedPhone").alias("Phone"),
        col("cbpOrgID").alias("cbpid"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On")
      )
    show(df)

    df = df.coalesce(1)
    val reportPathCBP = s"standalone-reports/all-assessment-report-cbp/${today}"
    val reportPathMDO = s"standalone-reports/all-assessment-report-mdo/${today}"
    // generateFullReport(df, reportPath)
    // df = df.drop("userID", "assessID", "assessOrgID")

    generateReports(df.drop("mdoid").withColumn("mdoid", col("cbpid")), "mdoid", s"/tmp/${reportPathCBP}", "AllAssessmentReportCBP")
    // generateAndSyncReports(df.drop("mdoid"), "cbpid", reportPathCBP, "AllAssessmentReportCBP")

    generateReports(df.drop("cbpid"), "mdoid", s"/tmp/${reportPathMDO}", "AllAssessmentReportMDO")
    // generateAndSyncReports(df.drop("cbpid"), "mdoid", reportPathMDO, "AllAssessmentReportMDO")

    closeRedisConnect()

  }

}