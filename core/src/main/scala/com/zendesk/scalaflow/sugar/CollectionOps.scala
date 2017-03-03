package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{KV, PCollection, PCollectionList, POutput}
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import WrapperOps._
import com.google.cloud.dataflow.sdk.coders.Coder

trait CollectionOps {

  implicit class RichCollection[A: TypeTag : Coder](val collection: PCollection[A]) {
    def parDo[B: TypeTag](f: DoFn[A, B]#ProcessContext => Unit)(implicit coder: Coder[B]): PCollection[B] = {
      collection.apply(asParDo(f)).setCoder(coder)
    }

    def map[B: TypeTag : Coder](f: A => B): PCollection[B] = parDo {
      c => c.output(f(c.element))
    }

    def filter(f: A => Boolean): PCollection[A] = parDo {
      c => if (f(c.element)) c.output(c.element)
    }

    def collect[B: TypeTag : Coder](pf: PartialFunction[A, B]): PCollection[B] = parDo {
      c => if (pf.isDefinedAt(c.element)) c.output(pf(c.element))
    }

    def extractTimestamp(implicit c: Coder[(A, Instant)]): PCollection[(A, Instant)] = parDo {
      c => c.output((c.element, c.timestamp))
    }

    def flatMap[B: TypeTag : Coder](f: A => Iterable[B]): PCollection[B] = parDo {
      c => f(c.element).foreach(c.output)
    }

    def foreach(f: A => Unit): PCollection[A] = parDo {
      c => { f(c.element); c.output(c.element) }
    }

    def withKey[B: TypeTag : Coder](f: A => B)(implicit c: Coder[KV[B, A]]): PCollection[KV[B, A]] = parDo {
      c => c.output(KV.of(f(c.element), c.element))
    }

    def flattenWith(first: PCollection[A], others: PCollection[A]*): PCollection[A] = {
      val all  = collection :: first :: others.toList
      PCollectionList.of(all.asJava).apply(Flatten.pCollections[A])
    }

    def transformWith[B <: POutput](name: String)(f: PCollection[A] => B): B = {
      collection.apply(name, asPTransform(f))
    }
  }
}

object CollectionOps extends CollectionOps
