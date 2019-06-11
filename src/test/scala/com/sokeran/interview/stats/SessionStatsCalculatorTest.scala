package com.sokeran.interview.stats

import com.sokeran.interview.enrichment.{SessionEnricher, SqlWindowSessionEnricher, TestDataLoader}
import com.sokeran.interview.model.Event.Columns
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class SessionStatsCalculatorTest extends WordSpec with BeforeAndAfterAll with Matchers {
  val sessionLengthSec = 300

  val session: SparkSession = SparkSession.builder()
    .appName("interview-task-tests")
    .master("local[1]")
    .getOrCreate()

  val enricher: SessionEnricher = new SqlWindowSessionEnricher

  "all medians must be correct" in {
    val tableName = readAllDataEnriched()
    SessionStatsCalculator.medianSessionDurationPerCategory(session, tableName).show()
    val medians = SessionStatsCalculator.medianSessionDurationPerCategory(session, tableName).collect()

    assertResult(3)(medians.length)
    assertMedianValue(medians, "books", 299.0)
    assertMedianValue(medians, "notebooks", 31.0)
    assertMedianValue(medians, "mobile phones", 17.0)
  }

  "all session groups must be correct" in {
    val tableName = readAllDataEnriched()
    SessionStatsCalculator.uniqueUsersPerSessionGroup(session, tableName, 60, 300).show()
    val groups = SessionStatsCalculator.uniqueUsersPerSessionGroup(session, tableName, 60, 300).collect()

    assertResult(6)(groups.length)
    assertSessionGroups(groups, "books", 2, 1)
    assertSessionGroups(groups, "books", 3, 1)
    assertSessionGroups(groups, "mobile phones", 1, 2)
    assertSessionGroups(groups, "mobile phones", 2, 1)
    assertSessionGroups(groups, "notebooks", 1, 2)
    assertSessionGroups(groups, "notebooks", 2, 2)
  }

  "all ranked products must be correct" in {
    val tableName = readAllData()
    SessionStatsCalculator.topProducts(session, tableName, 10).show()
    val ranks = SessionStatsCalculator.topProducts(session, tableName, 10).collect()

    assertResult(10)(ranks.length)
    assertProductRank(ranks, "books", "Scala for Dummies", 1, 213)
    assertProductRank(ranks, "books", "Sherlock Holmes, full collection", 2, 110)
    assertProductRank(ranks, "books", "Java for Dummies", 3, 36)
    assertProductRank(ranks, "books", "Romeo and Juliet", 4, 0)
    assertProductRank(ranks, "notebooks", "MacBook Air", 1, 312)
    assertProductRank(ranks, "notebooks", "MacBook Pro 13", 2, 143)
    assertProductRank(ranks, "notebooks", "MacBook Pro 15", 3, 62)
    assertProductRank(ranks, "mobile phones", "iPhone 8", 1, 309)
    assertProductRank(ranks, "mobile phones", "iPhone 8 Plus", 2, 25)
    assertProductRank(ranks, "mobile phones", "iPhone X", 3, 17)
  }

  def readAllDataEnriched(): String = {
    val (tableName, _) = TestDataLoader.readDf(session, "all.csv")
    val eventsTableName = "events"
    enricher.enrichEvents(session, tableName, sessionLengthSec)
      .createOrReplaceTempView(eventsTableName)
    eventsTableName
  }

  def readAllData(): String = {
    val (tableName, _) = TestDataLoader.readDf(session, "all.csv")
    tableName
  }

  def assertMedianValue(rows: Array[Row], category: String, expectedValue: Double): Unit = {
    val row = rows.find(r => r.getAs[String](Columns.category) == category)
    assert(row.isDefined, s"'$category $expectedValue'")
    val actualValue = row.get.getAs[Double]("medianSessionDurationSec")
    assertResult(expectedValue)(actualValue)
  }

  def assertSessionGroups(rows: Array[Row], category: String, sessionGroup: Int, expectedCount: Long): Unit = {
    val row = rows.find(r => r.getAs[String](Columns.category) == category &&
                        r.getAs[Int]("sessionGroup") == sessionGroup)
    assert(row.isDefined, s"'$category $sessionGroup $expectedCount'")
    val actualCount = row.get.getAs[Long]("cnt")
    assertResult(expectedCount)(actualCount)
  }

  def assertProductRank(rows: Array[Row], category: String, product: String, rank: Int, expectedTotalDurationSec: Long): Unit = {
    val row = rows.find(r => r.getAs[String](Columns.category) == category &&
                             r.getAs[String](Columns.product) == product &&
                             r.getAs[Int]("rank") == rank)

    assert(row.isDefined, s"'$category $product $rank $expectedTotalDurationSec'")
    val actualTotalDuration = row.get.getAs[Long]("totalDurationSec")
    assertResult(expectedTotalDurationSec)(actualTotalDuration)
  }
}
