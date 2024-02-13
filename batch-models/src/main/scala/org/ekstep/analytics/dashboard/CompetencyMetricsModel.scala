package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework._
import DashboardUtil._
import DataUtil._
import org.apache.spark.sql.types.DoubleType
import org.joda.time.DateTime

import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.LocalDate

/*

Prerequisites(PR) -

PR01: user's expected competencies, declared competencies, and competency gaps
PR02: course competency mapping
PR03: user's course progress
PR04: course rating summary
PR05: all competencies from FRAC


Metric  PR      Type                Description

M2.08   1,2     Scorecard           Number of competencies mapped to MDO officials for which there is no CBP on iGOT
M2.11   1       Scorecard           Average number of competency gaps per officer in the MDO
M2.22   1       Scorecard           Average for MDOs: Average number of competency gaps per officer
M3.55   1       Bar-Graph           Total competency gaps in the MDO
M3.56   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
M3.57   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
M3.58   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

S3.13   1       Scorecard           Average competency gaps per user
S3.11   4       Leaderboard         Average user rating of CBPs
S3.14   1       Bar-Graph           Total competency gaps
S3.15   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
S3.16   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
S3.17   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

C1.01   5       Scorecard           Total number of CBPs on iGOT platform
C1.1    4       Scorecard           Use ratings averaged for ALL CBPs by the provider
C1.03   3       Scorecard           Number of officers who enrolled (defined as 10% completion) for the CBP in the last year
C1.04   2,3     Bar-Graph           CBP enrolment rate (for a particular competency)
C1.05   3       Scorecard           Number of officers who completed the CBP in the last year
C1.06   3       Leaderboard         CBP completion rate
C1.07   4       Leaderboard         average user ratings by enrolled officers for each CBP
C1.09   5       Scorecard           No. of CBPs mapped (by competency)

*/

/**
 * OL01 - user: expected_competency_count
 * OL02 - user: declared_competency_count
 * OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
 * OL04 - mdo: average_competency_declaration_rate
 * OL05 - user: competency gap count
 * OL06 - user: enrolled cbp count
 * OL08 - user: competency gaps enrolled percentage
 * OL09 - mdo: average competency gaps enrolled percentage
 * OL10 - user: completed cbp count
 * OL11 - user: competency gap closed count
 * OL12 - user: competency gap closed percent
 * OL13 - mdo: avg competency gap closed percent
 */

/**
 * Model for processing competency metrics
 */
object CompetencyMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

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

    val processingTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(timestamp)
    Redis.update("dashboard_update_time", processingTime)

    println("Spark Config:")
    println(spark.conf.getAll)

    // obtain and save user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val designationsDF = orgDesignationsDF(userOrgDF)
    Redis.dispatchDataFrame[String]("org_designations", designationsDF, "userOrgID", "org_designations", replace = false)

    // kafkaDispatch(withTimestamp(orgDF, timestamp), conf.orgTopic)
    kafkaDispatch(withTimestamp(userOrgDF, timestamp), conf.userOrgTopic)

    // obtain and save role count data
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)
    val roleCountDF = roleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)

    // obtain and save org role count data
    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
      {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
      "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    // org user details redis dispatch
    val (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap) = getOrgUserMaps(orgUserCountDF)
    val activeOrgCount = orgDF.where(expr("orgStatus=1")).count()
    val activeUserCount = userDF.where(expr("userStatus=1")).count()
    Redis.dispatch(conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    Redis.dispatch(conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    Redis.dispatch(conf.redisOrgNameKey, orgNameMap)
    Redis.update(conf.redisTotalRegisteredOfficerCountKey, activeUserCount.toString)
    Redis.update(conf.redisTotalOrgCountKey, activeOrgCount.toString)

    // update redis data for learner home page
    updateLearnerHomePageData(orgDF, userOrgDF, userCourseProgramCompletionDF)

    // update redis data for dashboards
    dashboardRedisUpdates(orgRoleCount, userDF, allCourseProgramDetailsWithRatingDF, allCourseProgramCompletionWithDetailsDF)

    // update redis data and dispatch to kafka for competency related data
    // comment out for now
    // dashboardCompetencyUpdates(timestamp, allCourseProgramCompetencyDF, allCourseProgramCompletionWithDetailsDF)

    Redis.closeRedisConnect()

  }

  def dashboardRedisUpdates(orgRoleCount: DataFrame, userDF: DataFrame, allCourseProgramDetailsWithRatingDF: DataFrame,
                            allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    import spark.implicits._
    // new redis updates - start
    // MDO onboarded, with atleast one MDO_ADMIN/MDO_LEADER
    val orgWithMdoAdminLeaderCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN', 'MDO_LEADER') AND count > 0")).select("orgID").distinct().count()
    val orgWithMdoAdminCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN') AND count > 0")).select("orgID").distinct().count()
    Redis.update("dashboard_org_with_mdo_admin_leader_count", orgWithMdoAdminLeaderCount.toString)
    Redis.update("dashboard_org_with_mdo_admin_count", orgWithMdoAdminCount.toString)

    // mdo-wise registered user count
    val activeUsersByMDODF = userDF.where(expr("userStatus=1")).groupBy("userOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_user_count_by_user_org", activeUsersByMDODF, "userOrgID", "count")

    // new users registered yesterday
    val usersRegisteredYesterdayDF = userDF
      .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long"))
      .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long"))
    show(usersRegisteredYesterdayDF, "usersRegisteredYesterdayDF")
    val usersRegisteredYesterdayCount = usersRegisteredYesterdayDF
      .where(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp and userStatus=1"))
      .count()
    Redis.update("dashboard_new_users_registered_yesterday", usersRegisteredYesterdayCount.toString)
    println(s"dashboard_new_users_registered_yesterday = ${usersRegisteredYesterdayCount}")

    // cbp-wise live/draft/review/retired/pending-publish course counts
    val allCourseDF = allCourseProgramDetailsWithRatingDF.where(expr("category='Course'"))
    val liveCourseDF = allCourseDF.where(expr("courseStatus='Live'"))
    val draftCourseDF = allCourseDF.where(expr("courseStatus='Draft'"))
    val reviewCourseDF = allCourseDF.where(expr("courseStatus='Review'"))
    val retiredCourseDF = allCourseDF.where(expr("courseStatus='Retired'"))
    val pendingPublishCourseDF = reviewCourseDF.where(expr("courseReviewStatus='Reviewed'"))

    val liveCourseCountByCBPDF = liveCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_live_course_count_by_course_org", liveCourseCountByCBPDF, "courseOrgID", "count")
    val draftCourseCountByCBPDF = draftCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_draft_course_count_by_course_org", draftCourseCountByCBPDF, "courseOrgID", "count")
    val reviewCourseCountByCBPDF = reviewCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_review_course_count_by_course_org", reviewCourseCountByCBPDF, "courseOrgID", "count")
    val retiredCourseCountByCBPDF = retiredCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_retired_course_count_by_course_org", retiredCourseCountByCBPDF, "courseOrgID", "count")
    val pendingPublishCourseCountByCBPDF = pendingPublishCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_pending_publish_course_count_by_course_org", pendingPublishCourseCountByCBPDF, "courseOrgID", "count")

    // MDO with at least one live course
    val orgWithLiveCourseCount = liveCourseDF.select("courseOrgID").distinct().count()
    Redis.update("dashboard_cbp_with_live_course_count", orgWithLiveCourseCount.toString)
    println(s"dashboard_cbp_with_live_course_count = ${orgWithLiveCourseCount}")

    // Average rating across all live courses with ratings, and by CBP
    val ratedLiveCourseDF = liveCourseDF.where(col("ratingAverage").isNotNull)
    val avgRatingOverall = ratedLiveCourseDF.agg(avg("ratingAverage").alias("ratingAverage")).select("ratingAverage").first().getDouble(0)
    Redis.update("dashboard_course_average_rating_overall", avgRatingOverall.toString)
    println(s"dashboard_course_average_rating_overall = ${avgRatingOverall}")

    val avgRatingByCBPDF = ratedLiveCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    show(avgRatingByCBPDF, "avgRatingByCBPDF")
    Redis.dispatchDataFrame[Double]("dashboard_course_average_rating_by_course_org", avgRatingByCBPDF, "courseOrgID", "ratingAverage")

    // enrollment/not-started/started/in-progress/completion count, live and retired courses
    val liveRetiredCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val currentDate = LocalDate.now()
    // Calculate twelve months ago
    val twelveMonthsAgo = currentDate.minusMonths(12)
    // Convert to LocalDateTime by adding a time component (midnight)
    val twelveMonthsAgoLocalDateTime = twelveMonthsAgo.atStartOfDay()
    // Get the epoch time in milliseconds with IST offset
    val twelveMonthsAgoEpochMillis = twelveMonthsAgoLocalDateTime.toEpochSecond(java.time.ZoneOffset.ofHoursMinutes(5, 30)) * 1000
    println(twelveMonthsAgoEpochMillis)
    val liveRetiredCourseEnrolmentsInLast12MonthsDF = allCourseProgramCompletionWithDetailsDF.where(expr(s"category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1 AND courseEnrolledTimestamp >= ${twelveMonthsAgoEpochMillis}"))
    // started + not-started = enrolled
    val liveRetiredCourseNotStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus=0"))
    val liveRetiredCourseStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus IN (1, 2)"))
    // in-progress + completed = started
    val liveRetiredCourseInProgressDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=1"))
    val liveRetiredCourseCompletedDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=2"))

    // do both count(*) and countDistinct(userID) aggregates at once
    val enrolmentCountDF = liveRetiredCourseEnrolmentDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val notStartedCountDF = liveRetiredCourseNotStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val startedCountDF = liveRetiredCourseStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val inProgressCountDF = liveRetiredCourseInProgressDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val completedCountDF = liveRetiredCourseCompletedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))

    // unique user counts
    val enrolmentUniqueUserCount = enrolmentCountDF.select("uniqueUserCount").first().getLong(0)
    val notStartedUniqueUserCount = notStartedCountDF.select("uniqueUserCount").first().getLong(0)
    val startedUniqueUserCount = startedCountDF.select("uniqueUserCount").first().getLong(0)
    val inProgressUniqueUserCount = inProgressCountDF.select("uniqueUserCount").first().getLong(0)
    val completedUniqueUserCount = completedCountDF.select("uniqueUserCount").first().getLong(0)

    Redis.update("dashboard_unique_users_enrolled_count", enrolmentUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_not_started_count", notStartedUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_started_count", startedUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_in_progress_count", inProgressUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_completed_count", completedUniqueUserCount.toString)
    println(s"dashboard_unique_users_enrolled_count = ${enrolmentUniqueUserCount}")
    println(s"dashboard_unique_users_not_started_count = ${notStartedUniqueUserCount}")
    println(s"dashboard_unique_users_started_count = ${startedUniqueUserCount}")
    println(s"dashboard_unique_users_in_progress_count = ${inProgressUniqueUserCount}")
    println(s"dashboard_unique_users_completed_count = ${completedUniqueUserCount}")

    // counts
    val enrolmentCount = enrolmentCountDF.select("count").first().getLong(0)
    val notStartedCount = notStartedCountDF.select("count").first().getLong(0)
    val startedCount = startedCountDF.select("count").first().getLong(0)
    val inProgressCount = inProgressCountDF.select("count").first().getLong(0)
    val completedCount = completedCountDF.select("count").first().getLong(0)

    Redis.update("dashboard_enrolment_count", enrolmentCount.toString)
    Redis.update("dashboard_not_started_count", notStartedCount.toString)
    Redis.update("dashboard_started_count", startedCount.toString)
    Redis.update("dashboard_in_progress_count", inProgressCount.toString)
    Redis.update("dashboard_completed_count", completedCount.toString)
    println(s"dashboard_enrolment_count = ${enrolmentCount}")
    println(s"dashboard_not_started_count = ${notStartedCount}")
    println(s"dashboard_started_count = ${startedCount}")
    println(s"dashboard_in_progress_count = ${inProgressCount}")
    println(s"dashboard_completed_count = ${completedCount}")

    // mdo-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByMDODF = liveRetiredCourseEnrolmentDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredCourseEnrolmentsInLast12MonthsByMDODF = liveRetiredCourseEnrolmentsInLast12MonthsDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "uniqueUserCount")
    Redis.dispatchDataFrame[Long]("dashboard_active_users_last_12_months_by_org", liveRetiredCourseEnrolmentsInLast12MonthsByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByMDODF = liveRetiredCourseNotStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_not_started_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByMDODF = liveRetiredCourseStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_started_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_started_unique_user_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByMDODF = liveRetiredCourseInProgressDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByMDODF = liveRetiredCourseCompletedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_completed_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "uniqueUserCount")

    // cbp-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByCBPDF = liveRetiredCourseEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByCBPDF = liveRetiredCourseNotStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_not_started_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByCBPDF = liveRetiredCourseStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_started_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_started_unique_user_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByCBPDF = liveRetiredCourseInProgressDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByCBPDF = liveRetiredCourseCompletedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_completed_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "uniqueUserCount")

    // courses enrolled/completed at-least once, only live courses
    val liveCourseEnrolmentDF = liveRetiredCourseEnrolmentDF.where(expr("courseStatus='Live'"))
    val liveCourseCompletedDF = liveRetiredCourseCompletedDF.where(expr("courseStatus='Live'"))

    val coursesEnrolledInDF = liveCourseEnrolmentDF.select("courseID").distinct()
    val coursesCompletedDF = liveCourseCompletedDF.select("courseID").distinct()

    val coursesEnrolledInIdList = coursesEnrolledInDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray
    val coursesCompletedIdList = coursesCompletedDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray

    val coursesEnrolledInCount = coursesEnrolledInIdList.length
    val coursesCompletedCount = coursesCompletedIdList.length

    Redis.update("dashboard_courses_enrolled_in_at_least_once", coursesEnrolledInCount.toString)
    Redis.update("dashboard_courses_completed_at_least_once", coursesCompletedCount.toString)
    Redis.update("dashboard_courses_enrolled_in_at_least_once_id_list", coursesEnrolledInIdList.mkString(","))
    Redis.update("dashboard_courses_completed_at_least_once_id_list", coursesCompletedIdList.mkString(","))
    println(s"dashboard_courses_enrolled_in_at_least_once = ${coursesEnrolledInCount}")
    println(s"dashboard_courses_completed_at_least_once = ${coursesCompletedCount}")

    // mdo-wise courses completed at-least once
    val liveCourseCompletedAtLeastOnceByMDODF = liveCourseCompletedDF.groupBy("userOrgID").agg(countDistinct("courseID").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_courses_completed_at_least_once_by_user_org", liveCourseCompletedAtLeastOnceByMDODF, "userOrgID", "count")

    // Top 5 Users - By course completion
    // SELECT userID, CONCAT(firstName, ' ', lastName) AS name, maskedEmail, COUNT(courseID) AS completed_count
    // FROM \"dashboards-user-course-program-progress\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus=2 $mdo$ GROUP BY 1, 2, 3 ORDER BY completed_count DESC LIMIT 5
    val top5UsersByCompletionByMdoDF = liveRetiredCourseCompletedDF
      .groupBy("userID", "fullName", "maskedEmail", "userOrgID", "userOrgName")
      .agg(count("courseID").alias("completedCount"))
      .groupByLimit(Seq("userOrgID"), "completedCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "userID", "fullName", "maskedEmail", "userOrgID", "userOrgName", "completedCount"))
      .orderBy(col("completedCount").desc)
      .groupBy("userOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    show(top5UsersByCompletionByMdoDF, "top5UsersByCompletionByMdoDF")
    Redis.dispatchDataFrame[String]("dashboard_top_5_users_by_completion_by_org", top5UsersByCompletionByMdoDF, "userOrgID", "jsonData")

    // Top 5 Courses - By completion
    // SELECT courseID, courseName, category, courseOrgName, COUNT(userID) AS enrolled_count,
    // SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END) AS not_started_count,
    // SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END) AS in_progress_count,
    // SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END) AS \"Course Completions\"
    // FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') $mdo$
    // GROUP BY 1, 2, 3, 4 ORDER BY \"Course Completions\" DESC LIMIT 5
    val top5CoursesByCompletionByMdoDF = liveRetiredCourseEnrolmentDF
      .groupBy("courseID", "courseName", "userOrgID", "userOrgName")
      .agg(
        count("userID").alias("enrolledCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("notStartedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("inProgressCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount")
      )
      .groupByLimit(Seq("userOrgID"), "completedCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "courseID", "courseName", "userOrgID", "userOrgName", "enrolledCount", "notStartedCount", "inProgressCount", "completedCount"))
      .orderBy(col("completedCount").desc)
      .groupBy("userOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    show(top5CoursesByCompletionByMdoDF, "top5CoursesByCompletionByMdoDF")
    Redis.dispatchDataFrame[String]("dashboard_top_5_courses_by_completion_by_org", top5CoursesByCompletionByMdoDF, "userOrgID", "jsonData")

    // Top 5 Courses - By user ratings
    // SELECT courseID, courseName, category, courseOrgName, ROUND(AVG(ratingAverage), 1) AS rating_avg, SUM(ratingCount) AS rating_count
    // FROM \"dashboards-course\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND ratingCount>0 AND ratingAverage<=5.0 AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2, 3, 4 ORDER BY rating_count * rating_avg DESC LIMIT 5
    val top5CoursesByRatingDF = ratedLiveCourseDF
      .where(expr("ratingCount>0 AND ratingAverage<=5.0"))
      .withColumn("ratingMetric", expr("ratingCount * ratingAverage"))
      .orderBy(col("ratingMetric").desc)
      .limit(5)
      .select(
        col("courseID"),
        col("courseName"),
        col("courseOrgName"),
        round(col("ratingAverage"), 1).alias("ratingAverage"),
        col("ratingCount")
      )
    show(top5CoursesByRatingDF, "top5CoursesByRatingDF")
    val top5CoursesByRatingJson = top5CoursesByRatingDF.toJSON.collectAsList().toString
    println(top5CoursesByRatingJson)
    Redis.update("dashboard_top_5_courses_by_rating", top5CoursesByRatingJson)

    // Top 5 MDOs - By course completion
    // SELECT userOrgID, userOrgName, COUNT(userID) AS completed_count FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus=2
    // GROUP BY 1, 2 ORDER BY completed_count DESC LIMIT 5
    val top5MdoByCompletionDF = liveRetiredCourseCompletedDF
      .groupBy("userOrgID", "userOrgName")
      .agg(count("courseID").alias("completedCount"))
      .orderBy(col("completedCount").desc)
      .limit(5)
      .select(
        col("userOrgID"),
        col("userOrgName"),
        col("completedCount")
      )
    show(top5MdoByCompletionDF, "top5MdoByCompletionDF")
    val top5MdoByCompletionJson = top5MdoByCompletionDF.toJSON.collectAsList().toString
    println(top5MdoByCompletionJson)
    Redis.update("dashboard_top_5_mdo_by_completion", top5MdoByCompletionJson)

    // Top 5 MDOs - By courses published
    // SELECT courseOrgID, courseOrgName, COUNT(courseID) AS published_count FROM \"dashboards-course\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2 ORDER BY published_count DESC LIMIT 5
    val top5MdoByLiveCoursesDF = liveCourseDF
      .groupBy("courseOrgID", "courseOrgName")
      .agg(count("courseID").alias("publishedCount"))
      .orderBy(col("publishedCount").desc)
      .limit(5)
      .select(
        col("courseOrgID"),
        col("courseOrgName"),
        col("publishedCount")
      )
    show(top5MdoByLiveCoursesDF, "top5MdoByLiveCoursesDF")
    val top5MdoByLiveCoursesJson = top5MdoByLiveCoursesDF.toJSON.collectAsList().toString
    println(top5MdoByLiveCoursesJson)
    Redis.update("dashboard_top_5_mdo_by_live_courses", top5MdoByLiveCoursesJson)

    // new redis updates - end
  }

  def updateLearnerHomePageData(orgDF: DataFrame, userOrgDF: DataFrame, userCourseProgramCompletionDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val (hierarchyDF, cbpDetailsWithCompDF, cbpDetailsDF,
    cbpDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Curated Program"))
    val cbpCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, cbpDetailsDF, userOrgDF)

    //We only want the date here as the intent is to run this part of the script only once a day. The competency metrics
    // script may run a second time if we run into issues and this function should be skipped in that case.
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val currentDateString = dateFormat.format(System.currentTimeMillis())

    val lastRunDate: String = Redis.get("lhp_lastRunDate")
    print("The last run date is " + lastRunDate + "\n")
    print("current Date is" + currentDateString + "\n")

    if(!lastRunDate.equals(currentDateString)) {
      learnerHPRedisCalculations(cbpCompletionWithDetailsDF, cbpDetailsWithRatingDF)
    } else {
      print("This is a second run today and the computation and redis key updates are not required")
    }

    print("the current date is :" + currentDateString + "\n")
    Redis.update("lhp_lastRunDate", currentDateString)

  }

  def learningHoursDiff(learningHoursTillDay0: DataFrame, learningHoursTillDay1: DataFrame, defaultLearningHours: DataFrame, prefix: String): DataFrame = {
    if (learningHoursTillDay0.isEmpty) {
      return defaultLearningHours
    }

    learningHoursTillDay1
      .withColumnRenamed("totalLearningHours", "learningHoursTillDay1")
      .join(learningHoursTillDay0.withColumnRenamed("totalLearningHours", "learningHoursTillDay0"), Seq("userOrgID"), "left")
      .na.fill(0.0, Seq("learningHoursTillDay0", "learningHoursTillDay1"))
      .withColumn("totalLearningHours", expr("learningHoursTillDay1 - learningHoursTillDay0"))
      .withColumn(s"userOrgID", concat(col("userOrgID"), lit(s":${prefix}")))
      .select(s"userOrgID", "totalLearningHours")
  }

  def processLearningHours(courseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // The courseDuration is coming in seconds from ES, so converting it to hours. Also courseProgress is number of leaf nodes
    // consumed and we should look at completion percentage as the % of learning hours
    val totalLearningHoursTillTodayByOrg = courseProgramCompletionWithDetailsDF
      .filter("userOrgID IS NOT NULL AND TRIM(userOrgID) != ''")
      .groupBy("userOrgID")
      .agg(sum(expr("(completionPercentage / 100) * courseDuration")).alias("totalLearningSeconds"))
      .withColumn("totalLearningHours", col("totalLearningSeconds") / 3600)
      .drop("totalLearningSeconds")
    show(totalLearningHoursTillTodayByOrg, "totalLearningHoursTillTodayByOrg")

    val totalLearningHoursTillYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillToday", Schema.totalLearningHoursSchema)
      .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))
    show(totalLearningHoursTillYesterdayByOrg, "totalLearningHoursTillYesterdayByOrg")

    val totalLearningHoursTillDayBeforeYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillYesterday", Schema.totalLearningHoursSchema)
      .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))
    show(totalLearningHoursTillDayBeforeYesterdayByOrg, "totalLearningHoursTillDayBeforeYesterdayByOrg")

    //one issue with the learningHoursDiff returning the totalLearningHoursTillTodayByOrg as default if the 1st input DF is
    // empty implies that there would be entries in lhp_learningHours for "orgid":"hours", but I dont see an issue with this
    // I have unit tested the learning hours computation and it looks fine as long as it is run only once a day which we achieving
    // through the lhp_lastRunDate redis key
    val totalLearningHoursTodayByOrg = learningHoursDiff(totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, totalLearningHoursTillTodayByOrg, "today")
    show(totalLearningHoursTodayByOrg, "totalLearningHoursTodayByOrg")

    val totalLearningHoursYesterdayByOrg = learningHoursDiff(totalLearningHoursTillDayBeforeYesterdayByOrg, totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, "yesterday")
    show(totalLearningHoursYesterdayByOrg, "totalLearningHoursYesterdayByOrg")

    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillToday", totalLearningHoursTillTodayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillYesterday", totalLearningHoursTillYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursTodayByOrg, "userOrgID", "totalLearningHours", replace = false)

    // over all
    val totalLearningHoursYesterday: String = Redis.getMapField("lhp_learningHours", "across:today")
    val totalLearningHoursToday: String = totalLearningHoursTodayByOrg.agg(sum("totalLearningHours")).first().getDouble(0).toString

    println("learning across yesterday :" + totalLearningHoursYesterday)
    Redis.updateMapField("lhp_learningHours", "across:yesterday", totalLearningHoursYesterday)
    println("learning across today :" + totalLearningHoursToday)
    Redis.updateMapField("lhp_learningHours", "across:today", totalLearningHoursToday)
  }

  def processCertifications(courseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val totalCertificationsTillToday = courseProgramCompletionWithDetailsDF
      .where(expr("courseStatus IN ('Live') AND userStatus=1 AND dbCompletionStatus = 2 AND issuedCertificateCount > 0")).count()

    val totalCertificationsTillYesterdayStr = Redis.get("lhp_certificationsTillToday")
    val totalCertificationsTillYesterday = if (totalCertificationsTillYesterdayStr == "") { 0L } else { totalCertificationsTillYesterdayStr.toLong }
    val totalCertificationsTillDayBeforeYesterdayStr = Redis.get("lhp_certificationsTillYesterday")
    val totalCertificationsTillDayBeforeYesterday = if (totalCertificationsTillDayBeforeYesterdayStr == "") { 0L } else { totalCertificationsTillDayBeforeYesterdayStr.toLong }

    val totalCertificationsToday = totalCertificationsTillToday - totalCertificationsTillYesterday
    val totalCertificationsYesterday = totalCertificationsTillYesterday - totalCertificationsTillDayBeforeYesterday

    val currentDayStart = DateTime.now().withTimeAtStartOfDay()

    // The courseCompletionTimestamp is a toLong while getting the completedOn timestamp col from cassandra and it has
    // epoch seconds and not milliseconds. So converting the below to seconds
    val endOfCurrentDay = currentDayStart.plusDays(1).getMillis / 1000
    val startOf7thDay = currentDayStart.minusDays(7).getMillis / 1000

    val certificationsOfTheWeek = courseProgramCompletionWithDetailsDF
      .where(expr(s"courseStatus IN ('Live') AND userStatus=1 AND courseCompletedTimestamp > '${startOf7thDay}' AND courseCompletedTimestamp < '${endOfCurrentDay}' AND dbCompletionStatus = 2 AND issuedCertificateCount > 0"))

    val topNCertifications = certificationsOfTheWeek
      .groupBy("courseID")
      .agg(count("*").alias("courseCount"))
      .orderBy(desc("courseCount"))
      .limit(10)

    println("certifications till today :" + totalCertificationsTillToday)
    Redis.update("lhp_certificationsTillToday", totalCertificationsTillToday.toString)
    println("certifications till yesterday :" + totalCertificationsTillYesterday)
    Redis.update("lhp_certificationsTillYesterday", totalCertificationsTillYesterday.toString)
    println("certifications across yesterday :" + totalCertificationsYesterday)
    Redis.updateMapField("lhp_certifications", "across:yesterday", totalCertificationsYesterday.toString)
    println("certifications across today :" + totalCertificationsToday)
    Redis.updateMapField("lhp_certifications", "across:today", totalCertificationsToday.toString)

    val courseIdsString = topNCertifications
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    print("trending certifications :" +courseIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:certifications", courseIdsString)
  }

  def processTrending(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val trendingCourses = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category = 'Course'")
      .groupBy("courseID")
      .agg(count("*").alias("enrollmentCount"))
      .orderBy(desc("enrollmentCount"))
    val totalCourseCount = trendingCourses.count()
    val courseLimitCount = (totalCourseCount * 0.10).toInt

    val trendingCourseIdsString = trendingCourses.limit(courseLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    val trendingPrograms = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category IN ('Blended Program', 'Curated Program')")
      .groupBy("courseID")
      .agg(count("*").alias("enrollmentCount"))
      .orderBy(desc("enrollmentCount"))
    val totalProgramCount = trendingPrograms.count()
    val programLimitCount = (totalProgramCount * 0.10).toInt

    val trendingProgramIdsString = trendingPrograms.limit(programLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    val trendingCoursesByOrg = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category = 'Course'")
      .groupBy("userOrgID", "courseID")
      .agg(count("*").alias("enrollmentCount"))
      .groupByLimit(Seq("userOrgID"), "enrollmentCount", 50, desc = true)
      .drop("enrollmentCount", "rowNum")

    val trendingCoursesListByOrg = trendingCoursesByOrg
      .groupBy("userOrgID")
      .agg(collect_list("courseID").alias("courseIds"))
      .withColumn("userOrgID:courses", expr("userOrgID"))
      .withColumn("trendingCourseList", concat_ws(",", col("courseIds")))
      .withColumn("userOrgID:courses", concat(col("userOrgID:courses"), lit(":courses")))
      .select("userOrgID:courses", "trendingCourseList")
      .filter(col("userOrgID:courses").isNotNull && col("userOrgID:courses") =!= "")

    val trendingProgramsByOrg = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category  IN ('Blended Program', 'Curated Program')")
      .groupBy("userOrgID", "courseID")
      .agg(count("*").alias("enrollmentCount"))
      .groupByLimit(Seq("userOrgID"), "enrollmentCount", 50, desc = true)
      .drop("enrollmentCount", "rowNum")

    val trendingProgramsListByOrg = trendingProgramsByOrg
      .groupBy("userOrgID")
      .agg(collect_list("courseID").alias("courseIds"))
      .withColumn("userOrgID:programs", expr("userOrgID"))
      .withColumn("trendingProgramList", concat_ws(",", col("courseIds")))
      .withColumn("userOrgID:programs", concat(col("userOrgID:programs"), lit(":programs")))
      .select("userOrgID:programs", "trendingProgramList")
      .filter(col("userOrgID:programs").isNotNull && col("userOrgID:programs") =!= "")

    val mostEnrolledTag = trendingCourses.limit(courseLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    print("trending courses :" +trendingCourseIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:courses", trendingCourseIdsString)
    print("trending programs :" +trendingProgramIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:programs", trendingProgramIdsString)
    show(trendingCoursesListByOrg, "trendingCoursesListByOrg")
    Redis.dispatchDataFrame[String]("lhp_trending", trendingCoursesListByOrg, "userOrgID:courses", "trendingCourseList", replace = false)
    show(trendingProgramsListByOrg, "trendingProgramsListByOrg")
    Redis.dispatchDataFrame[String]("lhp_trending", trendingProgramsListByOrg, "userOrgID:programs", "trendingProgramList", replace = false)
//
    print("most enrolled tag :" + mostEnrolledTag)
    Redis.update("lhp_mostEnrolledTag", mostEnrolledTag + "\n")
  }

  def learnerHPRedisCalculations(cbpCompletionWithDetailsDF: DataFrame, cbpDetailsWithRatingDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val courseProgramCompletionWithDetailsDF = cbpCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program')"))

    // do home page data update
    val cbpsUnder30minsDf = cbpDetailsWithRatingDF.where(expr("courseStatus='Live' and courseDuration < 1800 AND category IN ('Course', 'Program')") && !col("courseID").endsWith("_rc")).orderBy(desc("ratingAverage"))
    show(cbpsUnder30minsDf, "cbpsUnder30minsDf")
    val coursesUnder30mins = cbpsUnder30minsDf
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)
    Redis.updateMapField("lhp_trending", "across:under_30_mins", coursesUnder30mins)

    // calculate and save learning hours to redis
    processLearningHours(courseProgramCompletionWithDetailsDF)

    // calculate and save certifications to redis
    processCertifications(courseProgramCompletionWithDetailsDF)

    // calculate and save trending data
    processTrending(cbpCompletionWithDetailsDF.where(expr("category != 'Program'")))
  }

  def dashboardCompetencyUpdates(timestamp: Long, allCourseProgramCompetencyDF: DataFrame, allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val liveCourseCompetencyDF = liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    val expectedCompetencyDF = expectedCompetencyDataFrame()
    val expectedCompetencyWithCourseCountDF = expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF, liveCourseCompetencyDF)
    validate({expectedCompetencyDF.count()}, {expectedCompetencyWithCourseCountDF.count()}, "expectedCompetencyDF.count() should equal expectedCompetencyWithCourseCountDF.count()")
    kafkaDispatch(withTimestamp(expectedCompetencyWithCourseCountDF, timestamp), conf.expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    val declaredCompetencyDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(declaredCompetencyDF, timestamp), conf.declaredCompetencyTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    val fracCompetencyDF = fracCompetencyDataFrame()
    val fracCompetencyWithCourseCountDF = fracCompetencyWithCourseCountDataFrame(fracCompetencyDF, liveCourseCompetencyDF)
    val fracCompetencyWithDetailsDF = fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF, expectedCompetencyDF, declaredCompetencyDF)
    validate({fracCompetencyDF.count()}, {fracCompetencyWithDetailsDF.count()}, "fracCompetencyDF.count() should equal fracCompetencyWithDetailsDF.count()")
    kafkaDispatch(withTimestamp(fracCompetencyWithDetailsDF, timestamp), conf.fracCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    val competencyGapDF = competencyGapDataFrame(expectedCompetencyDF, declaredCompetencyDF)
    val competencyGapWithCompletionDF = competencyGapCompletionDataFrame(competencyGapDF, liveCourseCompetencyDF, allCourseProgramCompletionWithDetailsDF)  // add course completion status
    validate({competencyGapDF.count()}, {competencyGapWithCompletionDF.count()}, "competencyGapDF.count() should equal competencyGapWithCompletionDF.count()")
    kafkaDispatch(withTimestamp(competencyGapWithCompletionDF, timestamp), conf.competencyGapTopic)

    val liveRetiredCourseCompletionWithDetailsDF = liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF)

    // officer dashboard metrics redis dispatch
    // OL01 - user: expected_competency_count
    val userExpectedCompetencyCountDF = expectedCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userExpectedCompetencyCountDF, "OL01")
    Redis.dispatchDataFrame[Long](conf.redisExpectedUserCompetencyCount, userExpectedCompetencyCountDF, "userID", "count")

    // OL02 - user: declared_competency_count
    val userDeclaredCompetencyCountDF = declaredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"))
    show(userDeclaredCompetencyCountDF, "OL02")
    Redis.dispatchDataFrame[Long](conf.redisDeclaredUserCompetencyCount, userDeclaredCompetencyCountDF, "userID", "count")

    // OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
    val coveredCompetencyDF = expectedCompetencyDF.join(declaredCompetencyDF, Seq("userID", "competencyID"), "leftouter")
      .na.fill(0, Seq("declaredCompetencyLevel"))
      .where(expr("declaredCompetencyLevel >= expectedCompetencyLevel"))
    val userCoveredCompetencyCountDF = coveredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("coveredCount"))
    val userCompetencyCoverRateDF = userExpectedCompetencyCountDF.join(userCoveredCompetencyCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("coveredCount"))
      .withColumn("rate", expr("coveredCount / count"))
    show(userCompetencyCoverRateDF, "OL03")
    Redis.dispatchDataFrame[Double](conf.redisUserCompetencyDeclarationRate, userCompetencyCoverRateDF, "userID", "rate")

    // OL04 - mdo: average_competency_declaration_rate
    val orgCompetencyAvgCoverRateDF = userCompetencyCoverRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyAvgCoverRateDF, "OL04")
    Redis.dispatchDataFrame[Double](conf.redisOrgCompetencyDeclarationRate, orgCompetencyAvgCoverRateDF, "orgID", "rate")

    // OL05 - user: competency gap count
    val userCompetencyGapDF = competencyGapDF.where(expr("competencyGap > 0"))
    val userCompetencyGapCountDF = userCompetencyGapDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userCompetencyGapCountDF, "OL05")
    Redis.dispatchDataFrame[Long](conf.redisUserCompetencyGapCount, userCompetencyGapCountDF, "userID", "count")

    // OL06 - user: enrolled cbp count (IMPORTANT: excluding completed courses)
    val userCourseEnrolledDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus in ('started', 'in-progress')"))
    val userCourseEnrolledCountDF = userCourseEnrolledDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseEnrolledCountDF, "OL06")
    Redis.dispatchDataFrame[Long](conf.redisUserCourseEnrolmentCount, userCourseEnrolledCountDF, "userID", "count")

    // OL08 - user: competency gaps enrolled percentage (IMPORTANT: excluding completed ones)
    val userCompetencyGapEnrolledDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus in ('started', 'in-progress')"))
    val userCompetencyGapEnrolledCountDF = userCompetencyGapEnrolledDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("enrolledCount"))
    val userCompetencyGapEnrolledRateDF = userCompetencyGapCountDF.join(userCompetencyGapEnrolledCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("enrolledCount"))
      .withColumn("rate", expr("enrolledCount / count"))
    show(userCompetencyGapEnrolledRateDF, "OL08")
    Redis.dispatchDataFrame[Double](conf.redisUserCompetencyGapEnrolmentRate, userCompetencyGapEnrolledRateDF, "userID", "rate")

    // OL09 - mdo: average competency gaps enrolled percentage
    val orgCompetencyGapAvgEnrolledRateDF = userCompetencyGapEnrolledRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgEnrolledRateDF, "OL09")
    Redis.dispatchDataFrame[Double](conf.redisOrgCompetencyGapEnrolmentRate, orgCompetencyGapAvgEnrolledRateDF, "orgID", "rate")

    // OL10 - user: completed cbp count
    val userCourseCompletedDF = liveRetiredCourseCompletionWithDetailsDF.where(expr("completionStatus = 'completed'"))
    val userCourseCompletedCountDF = userCourseCompletedDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseCompletedCountDF, "OL10")
    Redis.dispatchDataFrame[Long](conf.redisUserCourseCompletionCount, userCourseCompletedCountDF, "userID", "count")

    // OL11 - user: competency gap closed count
    val userCompetencyGapClosedDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus = 'completed'"))
    val userCompetencyGapClosedCountDF = userCompetencyGapClosedDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("closedCount"))
    show(userCompetencyGapClosedCountDF, "OL11")
    Redis.dispatchDataFrame[Long](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedCountDF, "userID", "closedCount")

    // OL12 - user: competency gap closed percent
    val userCompetencyGapClosedRateDF = userCompetencyGapCountDF.join(userCompetencyGapClosedCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("closedCount"))
      .withColumn("rate", expr("closedCount / count"))
    show(userCompetencyGapClosedRateDF,  "OL12")
    Redis.dispatchDataFrame[Double](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedRateDF, "userID", "rate")

    // OL13 - mdo: avg competency gap closed percent
    val orgCompetencyGapAvgClosedRateDF = userCompetencyGapClosedRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgClosedRateDF, "OL13")
    Redis.dispatchDataFrame[Double](conf.redisOrgCompetencyGapClosedRate, orgCompetencyGapAvgClosedRateDF, "orgID", "rate")

  }

}

