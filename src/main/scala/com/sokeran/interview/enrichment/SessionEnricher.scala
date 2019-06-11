package com.sokeran.interview.enrichment

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SessionEnricher {
  /**
    * Enriches events with user sessions.
    * Events contain following columns: 'category', 'product', 'userId', 'eventTime', eventType'
    * @param sessionLengthSec default max length between two consequent requests to be on single session
    * @return enriched data frame with columns: 'category', 'product', 'userId', 'eventTime', eventType',
    *         'sessionId', 'sessionStartTime', 'sessionEndTime'
    */
  def enrichEvents(session: SparkSession, tableName: String, sessionLengthSec: Long): DataFrame
}
