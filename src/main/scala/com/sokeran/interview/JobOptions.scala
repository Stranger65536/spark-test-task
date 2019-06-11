package com.sokeran.interview

import picocli.CommandLine
import picocli.CommandLine.Option

object JobOptions {
  def parse(args: Array[String]): JobOptions = {
    val options = new JobOptions()
    new CommandLine(options).parse(args: _*)
    options
  }
}

class JobOptions {
  @Option(names = Array("-i", "--input"), required = true)
  var inputPath: String = _

  @Option(names = Array("-o", "--output-folder"), required = true)
  var outputFolder: String = _

  @Option(names = Array("-sl", "--session-length"),
    description = Array("Session length in seconds"))
  var sessionLengthSec: Long = 300

  @Option(names = Array("-g1", "--session-group-1"),
    description = Array("Session length for session group 1"))
  var sessionLengthGroupOneSec: Long = 60

  @Option(names = Array("-g2", "--session-group-2"),
    description = Array("Session length for session group 2"))
  var sessionLengthGroupTwoSec: Long = 300

  @Option(names = Array("-t", "--top-products"),
    description = Array("Count of top products that must be shown for top products per category"))
  var topProducts: Int = 10
}
