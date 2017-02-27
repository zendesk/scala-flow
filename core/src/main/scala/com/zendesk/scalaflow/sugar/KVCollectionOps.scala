package com.zendesk.scalaflow.sugar

import java.lang.{Iterable => JIterable}

import com.google.cloud.dataflow.sdk.transforms.{Combine, DoFn, GroupByKey}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import WrapperOps._

trait KVCollectionOps {

  implicit class RichKVCollection[K: TypeTag, A: TypeTag](val collection: PCollection[KV[K, A]]) {

    def parDo[B: TypeTag](f: DoFn[KV[K, A], KV[K, B]]#ProcessContext => Unit): PCollection[KV[K, B]] = {
      collection.apply(asParDo(f))
    }

    def mapValue[B: TypeTag](f: A => B): PCollection[KV[K, B]] = parDo {
      c => c.output(KV.of(c.element.getKey, f(c.element.getValue)))
    }

    def flatMapValue[B: TypeTag](f: A => Iterable[B]): PCollection[KV[K, B]] = parDo {
      c => f(c.element.getValue).foreach { value => c.output(KV.of(c.element.getKey, value)) }
    }

    def combinePerKey(zero: A)(f: (A, A) => A): PCollection[KV[K, A]] = {
      val g = (input: JIterable[A]) => input.asScala.fold(zero)(f)
      collection.apply(Combine.perKey[K, A](asSimpleFn(g)))
    }

    def groupByKey: PCollection[KV[K, Iterable[A]]] = {
      collection.apply(GroupByKey.create[K, A]).mapValue(_.asScala)
    }

    def extractTimestamp: PCollection[KV[K, (A, Instant)]] = parDo {
      c => c.output(KV.of(c.element.getKey, (c.element.getValue, c.timestamp)))
    }
  }
}

object KVCollectionOps extends KVCollectionOps
