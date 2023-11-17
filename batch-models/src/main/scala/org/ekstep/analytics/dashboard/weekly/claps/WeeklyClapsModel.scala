package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object WeeklyClapsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable{

  implicit val className: String = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsModel"

  override def name() = "WeeklyClapsModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processWeeklyClaps(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processWeeklyClaps(l: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val (weekStart, weekEnd, today, yesterday) = getThisWeekDates()

    //get existing weekly-claps data
    val learnerDataDF = learnerStatsDataFrame()
    show(learnerDataDF, "learner data")

    //    val data = Seq(("190eac0c-86f8-41ed-bbac-be5ac0025191", 10.0, 1), ("21ce101d-f44f-4f8c-8810-9e0d4f5ae3c0", 75.0, 5), ("032d8653-5977-45f2-a4f1-a657efefb66b", 20.5, 2), ("12580399-2409-49c3-9dfc-8554711caebf", 9.0, 1))
    //    val columns = Seq("userid", "platformEngagementTime", "sessionCount")
    //    val platformEngagementDF: DataFrame = spark.createDataFrame(data).toDF(columns: _*)
    //    show(platformEngagementDF, "platform engagement")

    val platformEngagementDF = usersPlatformEngagementDataframe(weekStart)
    var df = learnerDataDF

    if(yesterday.equals(weekStart)) {
      df = df.select(
        col("w2").alias("w1"),
        col("w3").alias("w2"),
        col("w4").alias("w3"),
        col("total_claps"),
        col("userid")
      )
    } else {
      df = df.select(
        col("w1"),
        col("w2"),
        col("w3"),
        col("total_claps"),
        col("userid")
      )
    }

    df = df.join(platformEngagementDF, Seq("userid"), "full")
    show(df, "after full join ")

    df = df.withColumn("is_claps", lit(false))
      .withColumn("w4", map(lit("timespent"), col("platformEngagementTime"), lit("numberOfSessions"), col("sessionCount")))

    /**
     * total_claps
     * check >= cutoff everyday - total_claps +=1 if met
     * else wait till end of week and set to 0
     */

    writeToCassandra(df, conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)

  }
}
