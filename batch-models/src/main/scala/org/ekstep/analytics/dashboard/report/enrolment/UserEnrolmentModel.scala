package org.ekstep.analytics.dashboard.report.enrolment

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object UserEnrolmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentModel"

  override def name() = "UserEnrolmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserEnrolmentData(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config    model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processUserEnrolmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    //GET ORG DATA
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(orgHierarchyData, Seq("userOrgName"), "left")
    show(userDataDF, "userDataDF")

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    show(userEnrolmentDF, "userEnrolmentDF")

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userDataDF)
    show(allCourseProgramCompletionWithDetailsDF, "allCourseProgramCompletionWithDetailsDF")

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(expr("category IN ('Blended Program')"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val allCourseProgramCompletionWithDetailsWithBatchInfoDF = allCourseProgramCompletionWithDetailsDF.join(relevantBatchInfoDF, Seq("courseID", "batchID"), "left")
    show(allCourseProgramCompletionWithDetailsWithBatchInfoDF, "allCourseProgramCompletionWithDetailsWithBatchInfoDF")

    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsWithBatchInfoDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    var df = allCourseProgramCompletionWithDetailsDFWithRating
      .durationFormat("courseDuration")
      .withColumn("completedOn", to_date(col("courseCompletedTimestamp"), "dd/MM/yyyy"))
      .withColumn("enrolledOn", to_date(col("courseEnrolledTimestamp"), "dd/MM/yyyy"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), "dd/MM/yyyy"))
      .withColumn("Certificate_ID", col("certificateID"))

    df = df.distinct().dropDuplicates("userID", "courseID")

    val fullReportDF = df.select(
      col("userID"),
      col("userOrgID"),
      col("courseID"),
      col("courseOrgID"),
      col("fullName").alias("Full_Name"),
      col("professionalDetails.designation").alias("Designation"),
      col("personalDetails.primaryEmail").alias("Email"),
      col("personalDetails.mobile").alias("Phone_Number"),
      col("professionalDetails.group").alias("Group"),
      col("Tag"),
      col("ministry_name").alias("Ministry"),
      col("dept_name").alias("Department"),
      col("userOrgName").alias("Organization"),
      col("courseOrgName").alias("CBP_Provider"),
      col("courseName").alias("CBP_Name"),
      col("category").alias("CBP_Type"),
      col("courseDuration").alias("CBP_Duration"),
      col("batchID").alias("Batch_Id"),
      col("courseBatchName").alias("Batch_Name"),
      col("courseBatchStartDate").alias("Batch_Start_Date"),
      col("courseBatchEndDate").alias("Batch_End_Date"),
      col("enrolledOn").alias("Enrolled_On"),
      col("userCourseCompletionStatus").alias("Status"),
      col("completionPercentage").alias("CBP_Progress_Percentage"),
      col("courseLastPublishedOn").alias("Last_Published_On"),
      col("ArchivedOn").alias("CBP_Retired_On"),
      col("completedOn").alias("Completed_On"),
      col("Certificate_Generated"),
      col("userRating").alias("User_Rating"),
      col("personalDetails.gender").alias("Gender"),
      col("personalDetails.category").alias("Category"),
      col("additionalProperties.externalSystem").alias("External_System"),
      col("additionalProperties.externalSystemId").alias("External_System_Id"),
      col("userOrgID").alias("mdoid"),
      col("issuedCertificateCount"),
      col("courseStatus"),
      col("courseResourceCount").alias("resourceCount"),
      col("courseProgress").alias("resourcesConsumed"),
      round(expr("CASE WHEN courseResourceCount=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"), 2).alias("rawCompletionPercentage"),
      col("Certificate_ID"),
      col("Report_Last_Generated_On")
    )
      .coalesce(1)

    show(df, "df")

    val reportPath = s"${conf.userEnrolmentReportPath}/${today}"
    generateFullReport(fullReportDF, reportPath, conf.localReportDir)

    val mdoReportDF = fullReportDF.drop("userID", "userOrgID", "courseID", "courseOrgID", "issuedCertificateCount", "courseStatus", "resourceCount", "resourcesConsumed", "rawCompletionPercentage")
    generateAndSyncReports(mdoReportDF, "mdoid", reportPath, "ConsumptionReport")

    val warehouseDF = df
      .withColumn("certificate_generated_on", to_date(col("certificateGeneratedOn"), "yyyy-MM-dd"))
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .select(
        col("userID").alias("user_id"),
        col("batchID").alias("batch_id"),
        col("courseID").alias("cbp_id"),
        col("completionPercentage").alias("cbp_progress_percentage"),
        col("completedOn").alias("completed_on"),
        col("Certificate_Generated").alias("certificate_generated"),
        col("certificate_generated_on"),
        col("userRating").alias("user_rating"),
        col("courseProgress").alias("resource_count_consumed"),
        col("enrolledOn").alias("enrolled_on"),
        col("userCourseCompletionStatus").alias("user_consumption_status"),
        col("Certificate_ID").alias("certificate_id"),
        col("data_last_generated_on")
      )
    generateWarehouseReport(warehouseDF.coalesce(1), reportPath, conf.localReportDir)

    Redis.closeRedisConnect()

  }
}
