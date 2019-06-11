package com.sokeran.interview.stats

import org.apache.spark.sql.{DataFrame, SparkSession}

object SessionStatsCalculator {
  def medianSessionDurationPerCategory(session: SparkSession, sessionsTableName: String): DataFrame = {
    session.sql(
      s"""
         | select category, percentile(sessionDurationSec, 0.5) medianSessionDurationSec
         | from (
         |   select category, sessionId,
         |     (unix_timestamp(max(sessionEndTime)) - unix_timestamp(min(sessionStartTime))) sessionDurationSec
         |   from $sessionsTableName
         |   group by category, sessionId
         | )
         | group by category
      """.stripMargin)
  }


  def uniqueUsersPerSessionGroup(session: SparkSession, sessionsTableName: String,
                                 firstSessionGroupSec: Long, secondSessionGroupSec: Long): DataFrame = {
    session.sql(
      s"""
         | select category, sessionGroup, count(*) cnt from (
         |   select category, userId,
         |     case when sessionDurationSec < $firstSessionGroupSec then 1
         |          when sessionDurationSec < $secondSessionGroupSec then 2
         |          else 3 end as sessionGroup
         |   from (
         |     select category, userId,
         |       (unix_timestamp(max(sessionEndTime)) - unix_timestamp(min(sessionStartTime))) sessionDurationSec
         |     from $sessionsTableName
         |     group by category, userId, sessionId
         |   )
         | )
         | group by category, sessionGroup
         | order by category, sessionGroup, cnt
         |
       """.stripMargin
    )
  }

  def topProducts(session: SparkSession, eventsTableName: String, topProducts: Int): DataFrame = {
    val sqlSessionId =
      s"""
         | select *,
         |   sum(isNewSession)
         |     over (partition by userId order by eventTime rows between unbounded preceding and current row) as sessionId
         |   from (
         |     select *,
         |       case when lag(product, 1) over (partition by userId order by eventTime) is null then 1
         |            when lag(product, 1) over (partition by userId order by eventTime) != product then 1
         |            else 0 end as isNewSession
         |     from $eventsTableName
         |   )
       """.stripMargin

    val sqlSessionDuration =
      s"""
         | select category, product, userId, sessionId,
         |  (unix_timestamp(max(eventTime)) - unix_timestamp(min(eventTime))) as sessionDurationSec,
         |  row_number() over (partition by category, product order by sessionId) as productSessionNumber
         | from ($sqlSessionId)
         | group by category, product, userId, sessionId
       """.stripMargin

    session.sql(
      s"""
         | select category, product, rank, totalDurationSec
         | from (
         |   select category, product, totalDurationSec,
         |     dense_rank() over (partition by category order by totalDurationSec desc) rank
         |   from (
         |     select category, product, userId, sum(sessionDurationSec) totalDurationSec
         |     from ($sqlSessionDuration)
         |     group by category, product, userId
         |   )
         | )
         | where rank <= $topProducts
      """.stripMargin)
  }
}



