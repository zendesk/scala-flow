package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection, TupleTag}

import scala.collection.JavaConverters._

import CoderOps._
import TupleOps._

trait JoinOps {

  implicit class JoinPCollection[K : Coder, V1 : Coder](collection: PCollection[KV[K, V1]]) {

    def coGroupByKey[V2 : Coder](other: PCollection[KV[K, V2]]): PCollection[KV[K, (Iterable[V1], Iterable[V2])]] = {
      val tag1 = new TupleTag[V1]()
      val tag2 = new TupleTag[V2]()

      val tuple = KeyedPCollectionTuple.of(tag1, collection).and(tag2, other)

      val doFn = new DoFn[KV[K, CoGbkResult], KV[K, (Iterable[V1], Iterable[V2])]] {
        override def processElement(c: DoFn[KV[K, CoGbkResult], KV[K, (Iterable[V1], Iterable[V2])]]#ProcessContext) = {
          val key = c.element().getKey
          val coGbkResult = c.element().getValue

          val values1 = coGbkResult.getAll(tag1).asScala
          val values2 = coGbkResult.getAll(tag2).asScala

          val result = KV.of(key, (values1, values2))

          c.output(result)
        }
      }

      val coder = implicitly[Coder[KV[K, (Iterable[V1], Iterable[V2])]]]

      tuple
        .apply(CoGroupByKey.create[K])
        .apply(ParDo.of(doFn))
        .setCoder(coder)
    }
  }
}

object JoinOps extends JoinOps
