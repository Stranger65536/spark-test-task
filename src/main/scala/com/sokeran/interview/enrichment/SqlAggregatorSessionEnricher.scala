package com.sokeran.interview.enrichment

import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlAggregatorSessionEnricher extends SessionEnricher {

  override def enrichEvents(session: SparkSession, tableName: String, sessionLengthSec: Long): DataFrame = {
    session.sql(
      s"""
         | select *,
         | min(eventTime) over(partition by sessionId) as sessionStartTime,
         | max(eventTime) over(partition by sessionId) as sessionEndTime
         | from (
         |   select *,
         |     calculate_session_id(eventTime) over (partition by category, userId order by eventTime
         | ) sessionId
         | from $tableName)
         | order by eventTime""".stripMargin)
      .cache()
  }
}
