package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.VarIntCoder
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.zendesk.scalaflow.sugar.Implicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Try

class RichKVCollectionSpec extends FlatSpec with Matchers {

  behavior of "Rich Collection"

  "flatMap" should "work with Option" in {
    val pipeline = testPipeline()
    val input = List("42", "yo", "13")
    val output: PCollection[KV[String, Int]] = pipeline
      .apply(Create.of(input.asJava))
      .map { value => KV.of(value, value) }
      .flatMapValue { x => Try(x.toInt).toOption }

    DataflowAssert.that(output).containsInAnyOrder(KV.of("42", 42), KV.of("13", 13))

    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)

    val pipeline = TestPipeline.fromOptions(pipelineOptions)
    pipeline.getCoderRegistry.registerCoder(classOf[Int], classOf[VarIntCoder])
    pipeline
  }
}
