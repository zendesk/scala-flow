package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.{PCollection, PDone, TupleTag, TupleTagList}

import scala.util.Try
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import WrapperOps._

trait MiscOps {

  implicit class RichStringCollection(val collection: PCollection[String]) {
    def writeToPubsub(topic: String): PDone = {
      collection.apply(PubsubIO.Write.topic(topic))
    }
  }

  implicit class RichTryCollection[A: TypeTag](val collection: PCollection[Try[A]]) {
    /**
      * Map only success values, keeping failures intact.
      */
    def mapSuccess[B](f: A => B)(implicit tag: TypeTag[Try[B]]): PCollection[Try[B]] = {
      val g = (c: DoFn[Try[A], Try[B]]#ProcessContext) => c.output(c.element.map(f))
      collection.apply(asParDo(g))
    }

    def flatMapSuccess[B](f: A => Try[B])(implicit tag: TypeTag[Try[B]]): PCollection[Try[B]] = {
      val g = (c: DoFn[Try[A], Try[B]]#ProcessContext) => c.output(c.element.flatMap(f))
      collection.apply(asParDo(g))
    }
  }

  implicit class RichParDo[A, B](parDo: ParDo.Bound[A, B]) {
    def withMultipleOutput[T](main: TupleTag[B])(sides: TupleTag[_]*): ParDo.BoundMulti[A, B] = {
      parDo.withOutputTags(main, TupleTagList.of(sides.toList.asJava))
    }
  }
}

object MiscOps extends MiscOps
