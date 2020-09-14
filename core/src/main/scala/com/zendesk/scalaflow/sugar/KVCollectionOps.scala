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

    /** Map elements using a function.
      *
      * Converts a `PCollection` of element type `KV[K, A]` to a `PCollection` of element type
      * `KV[K, B]` using a function from `A` to `B`.
      *
      * {{{
      *   val orders: PCollection[KV[CustomerId, Order]] = ...
      *
      *   val purchasedProducts: PCollection[KV[CustomerId, Product]] =
      *     orders.mapValue(order => order.product)
      * }}}
      *
      * @param f A function that converts from `A` to `B`.
      */
    def mapValue[B : Coder](f: A => B): PCollection[KV[K, B]] = parDo {
      c => c.output(KV.of(c.element.getKey, f(c.element.getValue)))
    }

    def flatMapValue[B : Coder](f: A => Iterable[B]): PCollection[KV[K, B]] = parDo {
      c => f(c.element.getValue).foreach { value => c.output(KV.of(c.element.getKey, value)) }
    }

    /** Extract the timestamp from each element.
      *
      * {{{
      *   val orders: PCollection[KV[CustomerId, Order]] = ...
      *
      *   orders
      *     .extractTimestamp
      *     .map { case (order, timestamp) => ... }
      * }}}
      *
      * @return A `PCollection` of `KV` elements with the values being tuples containing
      *         an element and its timestamp.
      */
    def extractTimestamp: PCollection[KV[K, (A, Instant)]] = parDo {
      c => c.output(KV.of(c.element.getKey, (c.element.getValue, c.timestamp)))
    }

    /** Combine elements that share the same key into a single value.
      *
      * {{{
      *   val orders: PCollection[KV[CustomerId, Order]] = ...
      *
      *   val totalSpend: PCollection[KV[CustomerId, Float]] = orders
      *     .map(_.totalPrice)
      *     .combinePerKey(0.0) { case (x, y) => x + y }
      * }}}
      *
      * @param zero The initial accumulator value.
      * @param f A function that combines two values.
      * @return A `PCollection` of `KV[K, A]` where each value has been combined.
      */
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
