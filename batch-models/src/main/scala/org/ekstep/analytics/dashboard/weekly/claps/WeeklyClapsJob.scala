package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object WeeklyClapsJob extends optional.Application with IJob{

  implicit val className = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsJob"

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, WeeklyClapsModel)
    JobLogger.log("Job Completed.")
  }
}
