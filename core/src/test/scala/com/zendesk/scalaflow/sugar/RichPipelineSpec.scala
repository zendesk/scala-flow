package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create
import com.zendesk.scalaflow._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class RichPipelineSpec extends FlatSpec with Matchers {

  behavior of "RichPipeline"

  it should "register coders for Scala primitives and core types" in {
    val pipeline = testPipeline()

    val output = pipeline
      .apply(Create.of("x"))
      // Primitives
      .map(x => 1)
      .map(x => 1L)
      .map(x => 1.0)
      // Tuples
      .map(x => (2, 2L))
      .map(x => (3, 3L, 3.0))
      // Option
      .map(x => Some("some").asInstanceOf[Option[String]])
      .map(x => Option.empty[Int])
      // Try
      .map(x => Try("yay"))
      .map(x => Try[String](throw new RuntimeException("boo")))
      // Either
      .map(x => Left("left").asInstanceOf[Either[String, String]])
      .map(x => Right("right").asInstanceOf[Either[String, String]])

    DataflowAssert.that(output).containsInAnyOrder(Right("right"))
    pipeline.run()
  }


  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)
    TestPipeline.fromOptions(pipelineOptions)
  }
}
