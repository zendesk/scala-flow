package com.zendesk.scalaflow.sugar

import java.lang.{Iterable => JIterable}

import com.google.cloud.dataflow.sdk.coders.{Coder, IterableCoder}
import com.google.cloud.dataflow.sdk.transforms.{Combine, DoFn, GroupByKey, Top}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.zendesk.scalaflow._
import org.joda.time.Instant

import scala.collection.JavaConverters._

trait KVCollectionOps {

  implicit class RichKVCollection[K : Coder, A: Coder](val collection: PCollection[KV[K, A]]) {

    def parDo[B](f: DoFn[KV[K, A], KV[K, B]]#ProcessContext => Unit)(implicit coder: Coder[KV[K, B]]): PCollection[KV[K, B]] = {
      collection.apply(asParDo(f)).setCoder(coder)
    }

    def mapValue[B : Coder](f: A => B): PCollection[KV[K, B]] = parDo {
      c => c.output(KV.of(c.element.getKey, f(c.element.getValue)))
    }

    def flatMapValue[B : Coder](f: A => Iterable[B]): PCollection[KV[K, B]] = parDo {
      c => f(c.element.getValue).foreach { value => c.output(KV.of(c.element.getKey, value)) }
    }

    def extractTimestamp: PCollection[KV[K, (A, Instant)]] = parDo {
      c => c.output(KV.of(c.element.getKey, (c.element.getValue, c.timestamp)))
    }

    def combinePerKey(zero: A)(f: (A, A) => A): PCollection[KV[K, A]] = {
      val g = (input: JIterable[A]) => input.asScala.fold(zero)(f)
      collection.apply(Combine.perKey[K, A](asSimpleFn(g)))
    }

    def groupByKey: PCollection[KV[K, Iterable[A]]] = {
      collection.apply(GroupByKey.create[K, A]).mapValue(_.asScala)
    }

    def topPerKey(count: Int)(implicit ordered: Ordering[A]): PCollection[KV[K, List[A]]] = {
      collection.apply(Top.perKey(count, ordered)).mapValue(_.asScala.toList)
    }
  }
}

object KVCollectionOps extends KVCollectionOps
