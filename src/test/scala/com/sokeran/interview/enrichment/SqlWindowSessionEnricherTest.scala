package com.sokeran.interview.enrichment

class SqlWindowSessionEnricherTest extends EnricherAbstractTest {
  override def enricher: SessionEnricher = new SqlWindowSessionEnricher
}
