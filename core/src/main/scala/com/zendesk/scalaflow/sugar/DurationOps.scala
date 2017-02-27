package com.zendesk.scalaflow.sugar

import org.joda.time.Duration

trait DurationOps {

  implicit class IntWithTimeMethods(int: Int) {
    def second = Duration.standardSeconds(int)
    def seconds = Duration.standardSeconds(int)

    def minute = Duration.standardMinutes(int)
    def minutes = Duration.standardMinutes(int)

    def hour = Duration.standardHours(int)
    def hours = Duration.standardHours(int)

    def day = Duration.standardDays(int)
    def days = Duration.standardDays(int)
  }
}

object DurationOps extends DurationOps
