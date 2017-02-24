package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create

import org.scalatest.{FlatSpec, Matchers}

import CaseClassCoders._
import Implicits._

case class Foo()
case class Bar(name: String)
case class Qux(name: String, age: Int)

class CaseClassCoderSpec extends FlatSpec with Matchers {

  behavior of "CaseClassCoders"

  it should "handle zero member case class" in {
    val pipeline = testPipeline().registerCaseClass0(Foo)
    val output = pipeline
      .apply(Create.of(Foo()))
      .map(identity)

    DataflowAssert.that(output).containsInAnyOrder(Foo())
    pipeline.run()
  }

  it should "handle single member case class" in {
    val pipeline = testPipeline().registerCaseClass1(Bar)
    val output = pipeline
      .apply(Create.of(Bar("Fred")))
      .map(_.copy(name = "John"))

    DataflowAssert.that(output).containsInAnyOrder(Bar("John"))
    pipeline.run()
  }

  it should "handle double member case classes" in {
    val pipeline = testPipeline().registerCaseClass2(Qux)
    val output = pipeline
      .apply(Create.of(Qux("Fred", 27)))
      .map(_.copy(age = 35))

    DataflowAssert.that(output).containsInAnyOrder(Qux("Fred", 35))
    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)
    TestPipeline.fromOptions(pipelineOptions)
  }
}
