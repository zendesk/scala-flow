package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create
import com.zendesk.scalaflow._
import org.scalatest.{FlatSpec, Matchers}

class BranchOpsSpec extends FlatSpec with Matchers {

  "branch" should "branch into two collections" in {
    val pipeline = testPipeline()

    val input = pipeline.apply(Create.of(1, 2, 3, 4))

    val (even, odd) = input.branch(_ % 2 == 0, _ % 2 == 1)

    DataflowAssert.that(even).containsInAnyOrder(2, 4)
    DataflowAssert.that(odd).containsInAnyOrder(1, 3)

    pipeline.run()
  }

  "branch" should "branch into three collections" in {
    val pipeline = testPipeline()

    val input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6))

    val (a, b, c) = input.branch(_ % 3 == 0, _ % 3 == 1, _ % 3 == 2)

    DataflowAssert.that(a).containsInAnyOrder(3, 6)
    DataflowAssert.that(b).containsInAnyOrder(1, 4)
    DataflowAssert.that(c).containsInAnyOrder(2, 5)

    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)

    TestPipeline.fromOptions(pipelineOptions)
  }
}

