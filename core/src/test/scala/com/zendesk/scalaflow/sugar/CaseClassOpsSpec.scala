package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create

import org.scalatest.{FlatSpec, Matchers}

import CaseClassOps._

object CaseClassOpsSpec {
  case class Foo()
  case class Bar(name: String)
  case class Qux(name: String, age: Int)
  case class Wibble(foo: Foo, bar: Bar, qux: Qux)
}

class CaseClassOpsSpec extends FlatSpec with Matchers {
  import CaseClassOpsSpec._

  behavior of "CaseClassCoders"

  it should "handle zero member case class" in {
    val pipeline = testPipeline().registerCaseClass(Foo)
    val output = pipeline
      .apply(Create.of(Foo()))
      .map(identity)

    DataflowAssert.that(output).containsInAnyOrder(Foo())
    pipeline.run()
  }

  it should "handle single member case class" in {
    val pipeline = testPipeline().registerCaseClass(Bar)
    val output = pipeline
      .apply(Create.of(Bar("Fred")))
      .map(_.copy(name = "John"))

    DataflowAssert.that(output).containsInAnyOrder(Bar("John"))
    pipeline.run()
  }

  it should "handle double member case classes" in {
    val pipeline = testPipeline().registerCaseClass(Qux)
    val output = pipeline
      .apply(Create.of(Qux("Fred", 27)))
      .map(_.copy(age = 35))

    DataflowAssert.that(output).containsInAnyOrder(Qux("Fred", 35))
    pipeline.run()
  }

  it should "handle nested case classes" in {
    val pipeline = testPipeline()
      .registerCaseClass(Foo)
      .registerCaseClass(Bar)
      .registerCaseClass(Qux)
      .registerCaseClass(Wibble)

    val output = pipeline
      .apply(Create.of(Wibble(Foo(), Bar("John"), Qux("Fred", 27))))
      .map(_.copy(qux = Qux("Fred", 35)))

    DataflowAssert.that(output).containsInAnyOrder(Wibble(Foo(), Bar("John"), Qux("Fred", 35)))
    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)
    TestPipeline.fromOptions(pipelineOptions)
  }
}
