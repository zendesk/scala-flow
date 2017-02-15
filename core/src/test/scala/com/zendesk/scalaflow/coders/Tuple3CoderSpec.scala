package com.zendesk.scalaflow.coders

import java.lang.Long

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled
import com.google.cloud.dataflow.sdk.testing._
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.PCollection
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class Tuple3CoderSpec extends FlatSpec with Matchers {
  "Scala 3-tuples" should "be encoded and decoded in Dataflow" in {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(CheckEnabled.OFF)

    val pipeline = TestPipeline.fromOptions(pipelineOptions)
    val coderRegistry = pipeline.getCoderRegistry

    coderRegistry.registerCoder(classOf[Tuple3[_, _, _]], classOf[Tuple3Coder[_, _, _]])

    val data = Seq(
      ("john", 32: Long, "CA"),
      ("mike", 55: Long, "NY")
    )

    val input: PCollection[(String, Long, String)] = pipeline.apply(Create.of(data.toList.asJava))
    val output = input.apply(MapElements.via(new SimpleFunction[(String, Long, String), (String, Long, String)] {
      override def apply(tuple: (String, Long, String)): (String, Long, String) = (tuple._3, tuple._2, tuple._1)
    }))

    DataflowAssert
      .that(output)
      .containsInAnyOrder(
        ("CA", 32: Long, "john"),
        ("NY", 55: Long, "mike")
      )
  }
}
