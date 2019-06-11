package com.sokeran.interview.utils.logging

import org.slf4j.{Logger, LoggerFactory}

trait LazyTransientLogging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)
}
