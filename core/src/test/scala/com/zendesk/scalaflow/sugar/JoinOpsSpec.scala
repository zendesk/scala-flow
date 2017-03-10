package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.values.KV
import org.scalatest.{FlatSpec, Matchers}
import com.zendesk.scalaflow._

class JoinOpsSpec extends FlatSpec with Matchers {

  "coGroupByKey" should "join two collections" in {
    val pipeline = testPipeline()
    val input1 = pipeline.apply(Create.of(KV.of("x", 1), KV.of("y", 2), KV.of("x", 3)))
    val input2 = pipeline.apply(Create.of(KV.of("y", "yo"), KV.of("x", "lo")))

    val output = input1.coGroupByKey(input2)

    DataflowAssert.that(output).containsInAnyOrder(
      KV.of("x", (Set(1, 3), Set("lo"))),
      KV.of("y", (Set(2), Set("yo")))
    )

    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)

    val pipeline = TestPipeline.fromOptions(pipelineOptions)

    pipeline
  }
}
