package com.sokeran.interview.enrichment

import com.sokeran.interview.model.Event.{Columns, SessionColumns}
import com.sokeran.interview.utils.IdUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class SqlWindowSessionEnricher extends SessionEnricher {

  object TempColumns {
    val timestampDiff = "timestampDiff"
    val firstSessionId = "firstSessionId"
  }

  override def enrichEvents(session: SparkSession, tableName: String, sessionLengthSec: Long): DataFrame = {
    val events = session.table(tableName)
    val window = Window.partitionBy(Columns.category, Columns.userId).orderBy(Columns.eventTime)
    val udfGenerateId = udf(() => IdUtils.generateId())
    val sessions = events
      .withColumn(TempColumns.timestampDiff,
        unix_timestamp(col(Columns.eventTime)) - unix_timestamp(lag(Columns.eventTime, 1).over(window))
      )
      .withColumn(TempColumns.firstSessionId,
        when(col(TempColumns.timestampDiff) < sessionLengthSec, null).otherwise(udfGenerateId())
      )
      .withColumn(SessionColumns.sessionId,
        last(col(TempColumns.firstSessionId), ignoreNulls = true)
          .over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
      )
      .drop(TempColumns.timestampDiff, TempColumns.firstSessionId)

    val sessionWindow = Window.partitionBy(SessionColumns.sessionId)
    sessions
      .withColumn(SessionColumns.sessionStartTime, min(Columns.eventTime).over(sessionWindow))
      .withColumn(SessionColumns.sessionEndTime, max(Columns.eventTime).over(sessionWindow))
      .orderBy(Columns.eventTime)
      .cache()
  }
}
