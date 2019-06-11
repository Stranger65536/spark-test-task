package com.sokeran.interview

import com.sokeran.interview.enrichment.{CalculateSessionIdFunction, SqlWindowSessionEnricher}
import com.sokeran.interview.model.Event
import com.sokeran.interview.stats.SessionStatsCalculator
import com.sokeran.interview.utils.logging.LazyTransientLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object App extends LazyTransientLogging {
  def main(args: Array[String]): Unit = {
    val options: JobOptions = JobOptions.parse(args)

    val session: SparkSession = SparkSession.builder()
      .appName("interview-task")
      .getOrCreate()

    session.udf.register("calculate_session_id", new CalculateSessionIdFunction(options.sessionLengthSec))

    val eventsTableName = "events"
    val events: DataFrame = session.read
      .option("header", "true")
      .schema(Event.schema)
      .csv(options.inputPath)

    events.createOrReplaceTempView(eventsTableName)
    logger.info("Source data:")
    events.show()

    val enrichedEvents: DataFrame = new SqlWindowSessionEnricher().enrichEvents(session, eventsTableName, options.sessionLengthSec)
    enrichedEvents.write.csv(s"${options.outputFolder}/enriched.csv")
    logger.info("Enriched data:")
    enrichedEvents.show()

    val enrichedTableName = "sessions"
    enrichedEvents.createOrReplaceTempView(enrichedTableName)

    val medians = SessionStatsCalculator.medianSessionDurationPerCategory(session, enrichedTableName)
    medians.coalesce(1).write.csv(s"${options.outputFolder}/median")
    logger.info("Medians:")
    medians.show()

    val sessionGroups = SessionStatsCalculator.uniqueUsersPerSessionGroup(
      session, enrichedTableName, options.sessionLengthGroupOneSec, options.sessionLengthGroupTwoSec
    )
    sessionGroups.coalesce(1).write.csv(s"${options.outputFolder}/session_group")
    logger.info("Session groups:")
    sessionGroups.show()

    val topProducts = SessionStatsCalculator.topProducts(session, eventsTableName, options.topProducts)
    topProducts.coalesce(1).write.csv(s"${options.outputFolder}/top_products")
    logger.info("Top products per category:")
    topProducts.show()
  }
}
