package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.zendesk.scalaflow._
import org.scalatest.{FlatSpec, Matchers}

class CoderOpsSpec extends FlatSpec with Matchers {
  case class Person(name: String, age: Int)

  ".delegateCoder" should "create coders for case classes" in {
    val coder = CoderOps.delegateCoder[Person, (String, Int)](x => Person.unapply(x).get, x => Person.tupled(x))
    val input = Person("Molly", 99)

    val data = CoderUtils.encodeToBase64(coder, input)
    val output = CoderUtils.decodeFromBase64(coder, data)

    output shouldEqual input
  }
}
