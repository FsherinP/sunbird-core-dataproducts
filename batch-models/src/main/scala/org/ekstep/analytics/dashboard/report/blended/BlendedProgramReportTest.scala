package org.ekstep.analytics.dashboard.report.blended

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.framework.FrameworkContext

object BlendedProgramReportTest extends Serializable {

  def main(args: Array[String]): Unit = {

    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("BlendedProgramReportTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    BlendedProgramReportModel.processData(System.currentTimeMillis(), config)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "",
      "compression" -> "snappy",
      "topics" -> Map(
        "roleUserCount" -> "dev.dashboards.role.count",
        "orgRoleUserCount" -> "dev.dashboards.org.role.count",
        "allCourses" -> "dev.dashboards.course",
        "userCourseProgramProgress" -> "dev.dashboards.user.course.program.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap",
        "userOrg" -> "dev.dashboards.user.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "10.0.0.6",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "10.0.0.7",
      "sparkDruidRouterHost" -> "10.0.0.13",
      "sparkElasticsearchConnectionHost" -> "10.0.0.7",
      "fracBackendHost" -> "frac-dictionary-backend.igot-dev.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraRatingsTable" -> "ratings",
      "cassandraOrgHierarchyTable" -> "org_hierarchy",
      "cassandraCourseBatchTable" -> "course_batch",

      "store" -> "s3",
      "container" -> "igot",
      "key" -> "aws_storage_key",
      "secret" -> "aws_storage_secret",

      "mdoIDs" -> "0135071359030722569,01358993635114188855",

      "userReportPath" -> "standalone-reports/user-report",
      "userEnrolmentReportPath" -> "standalone-reports/user-enrollment-report",
      "courseReportPath" -> "standalone-reports/course-report",
      "cbaReportPath" -> "standalone-reports/cba-report",
      "taggedUsersPath" -> "tagged-users/",
      "standaloneAssessmentReportPath" -> "standalone-reports/user-assessment-report-cbp",
      "blendedReportPath" -> "standalone-reports/blended-program-report-mdo",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
