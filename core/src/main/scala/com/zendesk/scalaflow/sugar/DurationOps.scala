package com.zendesk.scalaflow.sugar

import java.time.{Duration => JavaDuration}
import org.joda.time.{Duration => JodaDuration}

import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.language.implicitConversions

trait DurationOps {
  implicit def java2joda(javaDuration: JavaDuration) = JodaDuration.millis(javaDuration.toMillis)
  implicit def scala2joda(scalaDuration: ScalaDuration) = JodaDuration.millis(scalaDuration.toMillis)
}

object DurationOps extends DurationOps
