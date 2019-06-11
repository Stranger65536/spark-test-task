package com.sokeran.interview.utils

import java.util.UUID

object IdUtils {
  def generateId(): String = UUID.randomUUID().toString
}
