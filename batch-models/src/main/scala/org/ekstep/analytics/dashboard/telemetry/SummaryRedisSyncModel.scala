package org.ekstep.analytics.dashboard.telemetry

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework._
import org.joda.time.DateTime
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._

import java.io.Serializable
import java.text.SimpleDateFormat

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
object SummaryRedisSyncModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.telemetry.SummaryRedisSyncModel"
  override def name() = "SummaryRedisSyncModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processData(timestamp, config)
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
  def processData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    println("Spark Config:")
    println(spark.conf.getAll)

    // Users active
    // SELECT dimension_channel, COUNT(DISTINCT(uid)) as active_count FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '12' MONTH GROUP BY 1
    //val activeUsersLast12MonthsDF = activeUsersLast12MonthsDataFrame()
    //Redis.dispatchDataFrame[Long]("dashboard_active_users_last_12_months_by_org", activeUsersLast12MonthsDF, "orgID", "activeCount")

    // Daily time spent by users
    // SELECT dimension_channel AS orgID, SUM(total_time_spent)/(30 * 3600.0) as timeSpent FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY GROUP BY 1
    val dailyTimeSpentLast30DaysDF = dailyTimeSpentLast30DaysDataFrame()
    Redis.dispatchDataFrame[Float]("dashboard_time_spent_last_30_days_by_org", dailyTimeSpentLast30DaysDF, "orgID", "timeSpent")

    // Daily active users
    // SELECT dimension_channel, COUNT(DISTINCT(actor_id)) as active_count FROM \"summary-events\"
    // WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR GROUP BY 1
    val activeUsersLast24HoursDF = activeUsersLast24HoursDataFrame()
    Redis.dispatchDataFrame[Long]("dashboard_active_users_last_24_hours_by_org", activeUsersLast24HoursDF, "orgID", "activeCount")


    Redis.closeRedisConnect()

  }

//  def activeUsersLast12MonthsDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
//    val query = """SELECT dimension_channel AS orgID, COUNT(DISTINCT(uid)) as activeCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '12' MONTH GROUP BY 1"""
//    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
//    if (df == null) return emptySchemaDataFrame(Schema.activeUsersSchema)
//
//    df = df.withColumn("activeCount", expr("CAST(activeCount as LONG)"))  // Important to cast as long otherwise a cast will fail later on
//
//    show(df)
//    df
//  }

  def dailyTimeSpentLast30DaysDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT dimension_channel AS orgID, SUM(total_time_spent)/(30 * 3600.0) as timeSpent FROM \"summary-events\" WHERE dimensions_type='app' AND __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY GROUP BY 1"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.timeSpentSchema)

    df = df.withColumn("timeSpent", expr("CAST(timeSpent as FLOAT)"))  // Important to cast as float otherwise a cast will fail later on

    show(df)
    df
  }

  def activeUsersLast24HoursDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT dimension_channel AS orgID, COUNT(DISTINCT(uid)) as activeCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR GROUP BY 1"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(Schema.activeUsersSchema)

    df = df.withColumn("activeCount", expr("CAST(activeCount as LONG)"))  // Important to cast as long otherwise a cast will fail later on

    show(df)
    df
  }

}
