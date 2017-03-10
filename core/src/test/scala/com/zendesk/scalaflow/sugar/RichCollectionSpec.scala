package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled._
import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.values.{KV, TimestampedValue}
import com.zendesk.scalaflow._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class RichCollectionSpec extends FlatSpec with Matchers {

  implicit val rangeCoder = serializableCoder[Range.Inclusive]

  behavior of "Rich Collection"

  it should "map values" in {
    val pipeline = testPipeline()
    val input = 10 to 12
    val output = pipeline.begin
      .transform(Create.of(input.asJava))
      .map(_.toHexString)

    DataflowAssert.that(output).containsInAnyOrder("a", "b", "c")
    pipeline.run()
  }

  it should "flat map iterables" in {
    val pipeline = testPipeline()
    val input = List(1 to 2, 3 to 5, 6 to 9)
    val output = pipeline.begin
      .transform(Create.of(input.asJava))
      .flatMap(identity)

    DataflowAssert.that(output).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9)
    pipeline.run()
  }

  it should "filter values" in {
    val pipeline = testPipeline()
    val input = 0 to 9
    val output = pipeline.begin
      .transform(Create.of(input.asJava))
      .filter(_ % 2 == 0)

    DataflowAssert.that(output).containsInAnyOrder(0, 2, 4, 6, 8)
    pipeline.run()
  }

  // FizzBuzz as a massively scalable streaming functional paradigm ;-)
  it should "collect values" in {
    val pipeline = testPipeline()
    val input = 1 to 15
    val output = pipeline.begin
      .transform(Create.of(input.asJava))
      .collect {
        case x if x % 3 == 0 => if (x % 5 == 0) "FizzBuzz" else "Fizz"
        case x if x % 5 == 0 => "Buzz"
      }

    DataflowAssert.that(output).containsInAnyOrder("Fizz", "Buzz", "Fizz", "Fizz", "Buzz", "Fizz", "FizzBuzz")
    pipeline.run()
  }

  it should "flatten PCollections together" in {
    val pipeline = testPipeline()
    val first = pipeline.begin.transform(Create.of((1 to 2).asJava))
    val second = pipeline.begin.transform(Create.of((3 to 5).asJava))
    val third = pipeline.begin.transform(Create.of((6 to 9).asJava))

    val output = first.flattenWith(second, third)

    DataflowAssert.that(output).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9)
    pipeline.run()
  }

  it should "use withKey to facilitate conversion to KV" in {
    val pipeline = testPipeline()
    val input = (1 to 3).map(_.toString)
    val output = pipeline.begin
      .transform(Create.of(input.asJava))
      .withKey("k" + _)

    DataflowAssert.that(output).containsInAnyOrder(KV.of("k1", "1"), KV.of("k2", "2"), KV.of("k3", "3"))
    pipeline.run()
  }

  it should "extract timestamps" in {
    val pipeline = testPipeline()

    val now = DateTime.now()
    val yesterday = now.minusDays(1).toInstant
    val today = now.toInstant()
    val tomorrow = now.plusDays(1).toInstant

    val input = List(TimestampedValue.of("yesterday", yesterday), TimestampedValue.of("today", today), TimestampedValue.of("tomorrow", tomorrow))
    val output = pipeline.begin
      .transform(Create.timestamped(input.asJava))
      .extractTimestamp

    DataflowAssert.that(output).containsInAnyOrder(("yesterday", yesterday), ("today", today), ("tomorrow", tomorrow))
    pipeline.run()
  }

  private def testPipeline() = {
    val pipelineOptions = TestPipeline.testingPipelineOptions
    pipelineOptions.setStableUniqueNames(OFF)

    TestPipeline.fromOptions(pipelineOptions)
  }
}
