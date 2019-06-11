package com.sokeran.interview.enrichment

import com.sokeran.interview.model.Event.{Columns, SessionColumns}
import com.sokeran.interview.utils.IdUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CalculateSessionIdFunction(sessionLengthSec: Long) extends UserDefinedAggregateFunction {
  private val inputEventTimeIndex = 0

  override def inputSchema: StructType = new StructType().add(Columns.eventTime, TimestampType)

  private val bufferPreviousTimeIndex = 0
  private val bufferSessionIdIndex = 1

  override def bufferSchema: StructType = new StructType()
    .add("previousTime", TimestampType)
    .add(SessionColumns.sessionId, StringType)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer.update(bufferSessionIdIndex, IdUtils.generateId())

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentTime = input.getTimestamp(inputEventTimeIndex)

    if (!buffer.isNullAt(bufferPreviousTimeIndex)) {
      val previousTime = buffer.getTimestamp(bufferPreviousTimeIndex)

      if (previousTime.getTime > currentTime.getTime) {
        throw new IllegalStateException(s"Previous time can't be greater than current time: $previousTime > $currentTime")
      }

      if (currentTime.getTime - previousTime.getTime > sessionLengthSec * 1000) {
        buffer.update(bufferSessionIdIndex, IdUtils.generateId())
      }
    }

    buffer.update(bufferPreviousTimeIndex, currentTime)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Nothing =
    throw new IllegalStateException("Function should be used only in window partitioning")

  override def evaluate(buffer: Row): Any = buffer.getString(bufferSessionIdIndex)
}
