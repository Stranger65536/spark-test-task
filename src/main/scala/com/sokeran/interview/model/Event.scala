package com.sokeran.interview.model

import org.apache.spark.sql.types._

object Event {

  object Columns {
    val category = "category"
    val product = "product"
    val userId = "userId"
    val eventTime = "eventTime"
    val eventType = "eventType"
  }

  object SessionColumns {
    val sessionId = "sessionId"
    val sessionStartTime = "sessionStartTime"
    val sessionEndTime = "sessionEndTime"
  }

  val schema = StructType(Seq(
    StructField(Columns.category, StringType, nullable = false),
    StructField(Columns.product, StringType, nullable = false),
    StructField(Columns.userId, StringType, nullable = false),
    StructField(Columns.eventTime, TimestampType, nullable = false),
    StructField(Columns.eventType, StringType, nullable = false)
  ))
}
