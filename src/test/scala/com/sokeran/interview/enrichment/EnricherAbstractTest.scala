package com.sokeran.interview.enrichment

import com.sokeran.interview.model.Event.SessionColumns
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

abstract class EnricherAbstractTest extends WordSpec with BeforeAndAfterAll with Matchers {
  val sessionLengthSec = 300

  val session: SparkSession = SparkSession.builder()
    .appName("interview-task-tests")
    .master("local[1]")
    .getOrCreate()

  session.udf.register("calculate_session_id", new CalculateSessionIdFunction(sessionLengthSec))

  def enricher: SessionEnricher

  "must be one session for single user" in {
    assertCorrectCalculations(TestDataLoader.readDf(session, "single_session.csv"))
  }

  "must be two sessions for single user" in {
    assertCorrectCalculations(TestDataLoader.readDf(session, "two_sessions.csv"))
  }

  "must be two separate sessions for two users" in {
    assertCorrectCalculations(TestDataLoader.readDf(session, "two_users.csv"))
  }

  def assertCorrectCalculations(tableNameAndExpectedDf: (String, DataFrame)): Unit = {
    val events = enricher.enrichEvents(session, tableNameAndExpectedDf._1, sessionLengthSec)
    events.show()
    tableNameAndExpectedDf._2.show()
    val expectedRows = tableNameAndExpectedDf._2.collect()
    val actualRows = events.drop(SessionColumns.sessionId).collect()
    assertResult(expectedRows)(actualRows)
  }
}
