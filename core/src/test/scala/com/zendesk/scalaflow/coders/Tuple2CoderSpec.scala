package com.zendesk.scalaflow.coders

import java.lang.Long

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled
import com.google.cloud.dataflow.sdk.testing._
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.PCollection
import com.zendesk.scalaflow.sugar.Implicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class Tuple2CoderSpec extends FlatSpec with Matchers {
  "Scala 2-tuples" should "be encoded and decoded in Dataflow" in {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(CheckEnabled.OFF)

    val pipeline = TestPipeline.fromOptions(pipelineOptions)
    val coderRegistry = pipeline.getCoderRegistry

    coderRegistry.registerCoder(classOf[Tuple2[_, _]], classOf[Tuple2Coder[_, _]])

    val data = Seq(
      ("john", 32: Long),
      ("mike", 55: Long)
    )

    val input: PCollection[(String, Long)] = pipeline.apply(Create.of(data.toList.asJava))
    val output = input.map { case (a, b) => (b, a) }

    DataflowAssert
      .that(output)
      .containsInAnyOrder(
        (32: Long, "john"),
        (55: Long, "mike")
      )
  }
}
