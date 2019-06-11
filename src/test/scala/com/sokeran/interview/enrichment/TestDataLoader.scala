package com.sokeran.interview.enrichment

import com.sokeran.interview.model.Event
import com.sokeran.interview.model.Event.{Columns, SessionColumns}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestDataLoader {
  type TableName = String
  type ExpectedResultDF = DataFrame

  private val testSchema = StructType(Seq(
    StructField(Columns.category, StringType, nullable = false),
    StructField(Columns.product, StringType, nullable = false),
    StructField(Columns.userId, StringType, nullable = false),
    StructField(Columns.eventTime, TimestampType, nullable = false),
    StructField(Columns.eventType, StringType, nullable = false),
    StructField(SessionColumns.sessionStartTime, TimestampType, nullable = false),
    StructField(SessionColumns.sessionEndTime, TimestampType, nullable = false)
  ))

  def readDf(session: SparkSession, name: String): (TableName, ExpectedResultDF) = {
    val path = Thread.currentThread().getContextClassLoader.getResource(s"data/$name").toString

    val inputDf = session.read
      .option("header", "true")
      .schema(Event.schema)
      .csv(path)

    val expectedDf = session.read
      .option("header", "true")
      .schema(testSchema)
      .csv(path)

    val tableName = RandomStringUtils.randomAlphabetic(10)
    inputDf.createOrReplaceTempView(tableName)
    (tableName, expectedDf)
  }
}
