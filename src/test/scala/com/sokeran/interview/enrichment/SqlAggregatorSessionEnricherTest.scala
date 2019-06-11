package com.sokeran.interview.enrichment

class SqlAggregatorSessionEnricherTest extends EnricherAbstractTest {
  override def enricher: SessionEnricher = new SqlAggregatorSessionEnricher
}
