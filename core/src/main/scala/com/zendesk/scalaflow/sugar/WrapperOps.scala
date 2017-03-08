package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.transforms.{DoFn, PTransform, ParDo, SimpleFunction}
import com.google.cloud.dataflow.sdk.values.{PInput, POutput}

trait WrapperOps {

  def asSimpleFn[A, B](f: A => B): SimpleFunction[A, B] = {
    new SimpleFunction[A, B] {
      override def apply(input: A): B = f(input)
    }
  }

  def asParDo[A, B](f: DoFn[A, B]#ProcessContext => Unit): ParDo.Bound[A, B] = {
    val doFn = new DoFn[A, B] {
      override def processElement(c: DoFn[A, B]#ProcessContext): Unit = f(c)
    }

    ParDo.of(doFn)
  }

  def asPTransform[A <: PInput, B <: POutput](f: A => B): PTransform[A, B] = {
    new PTransform[A, B] {
      override def apply(input: A): B = f(input)
    }
  }
}

object WrapperOps extends WrapperOps
