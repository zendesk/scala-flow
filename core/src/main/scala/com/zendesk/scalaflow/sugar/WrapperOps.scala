package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.transforms.{DoFn, PTransform, ParDo, SimpleFunction}

import scala.reflect.runtime.universe._
import TypeTagOps._
import com.google.cloud.dataflow.sdk.values.{PCollection, PInput, POutput}

trait WrapperOps {

  def asSimpleFn[A, B](f: A => B)(implicit tag: TypeTag[B]): SimpleFunction[A, B] = {
    // The underlying TypeTag code created by the Scala compiler closes around the containing classes,
    // so to ensure the SimpleFunction is serializable "asTypeDescriptor" needs to be called here,
    // then passed into the SimpleFunction as a by-value parameter
    val descriptor = tag.asTypeDescriptor

    new SimpleFunction[A, B] {
      override def apply(input: A): B = f(input)
      override def getOutputTypeDescriptor = descriptor
    }
  }

  def asParDo[A, B](f: DoFn[A, B]#ProcessContext => Unit)(implicit tag: TypeTag[B]): ParDo.Bound[A, B] = {
    // The underlying TypeTag  code created by the Scala compiler closes around the containing classes,
    // so to ensure the DoFn is serializable "asTypeDescriptor" needs to be called here,
    // then passed into the DoFn as a by-value parameter
    val descriptor = tag.asTypeDescriptor

    val doFn = new DoFn[A, B] {
      override def processElement(c: DoFn[A, B]#ProcessContext): Unit = f(c)
      override def getOutputTypeDescriptor = descriptor
    }

    ParDo.of(doFn)
  }

  def asPTransform[A <: PInput, B <: POutput](f: A => B) = new PTransform[A, B] {
    override def apply(input: A): B = f(input)
  }
}

object WrapperOps extends WrapperOps
