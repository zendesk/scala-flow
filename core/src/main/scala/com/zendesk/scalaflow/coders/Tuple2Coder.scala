package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}
import java.util

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{Coder, StandardCoder}
import com.google.cloud.dataflow.sdk.util.Structs.addBoolean
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver
import com.google.cloud.dataflow.sdk.util.{CloudObject, PropertyNames}

import scala.collection.JavaConverters._

case class Tuple2Coder[A, B](aCoder: Coder[A], bCoder: Coder[B]) extends StandardCoder[Tuple2[A, B]] {
  override def encode(value: (A, B), outStream: OutputStream, context: Context): Unit = {
    val nestedContext = context.nested

    aCoder.encode(value._1, outStream, nestedContext)
    bCoder.encode(value._2, outStream, nestedContext)
  }

  override def decode(inStream: InputStream, context: Context): (A, B) = {
    val nestedContext = context.nested
    val a = aCoder.decode(inStream, nestedContext)
    val b = bCoder.decode(inStream, nestedContext)

    (a, b)
  }

  def getCoder1(): Coder[A] = aCoder
  def getCoder2(): Coder[B] = bCoder

  override def consistentWithEquals(): Boolean = {
    aCoder.consistentWithEquals && bCoder.consistentWithEquals
  }

  override def getCoderArguments: util.List[_ <: Coder[_]] = {
    java.util.Arrays.asList(aCoder, bCoder)
  }

  override def verifyDeterministic(): Unit = {
    verifyDeterministic("First coder must be deterministic", aCoder)
    verifyDeterministic("Second coder must be deterministic", bCoder)
  }

  override def registerByteSizeObserver(value: (A, B), observer: ElementByteSizeObserver, context: Context): Unit = {
    aCoder.registerByteSizeObserver(value._1, observer, context.nested)
    bCoder.registerByteSizeObserver(value._2, observer, context.nested)
  }

  override def structuralValue(value: (A, B)): AnyRef = {
    if (consistentWithEquals)
      value
    else
      (aCoder.structuralValue(value._1), bCoder.structuralValue(value._2))
  }

  override def isRegisterByteSizeObserverCheap(value: (A, B), context: Context): Boolean = {
    aCoder.isRegisterByteSizeObserverCheap(value._1, context.nested) &&
      bCoder.isRegisterByteSizeObserverCheap(value._2, context.nested)
  }

  override def asCloudObject(): CloudObject = {
    val result = super.asCloudObject
    addBoolean(result, PropertyNames.IS_PAIR_LIKE, true)
    result
  }

  override def getEncodingId = "Tuple2Coder"
}

object Tuple2Coder {
  def of[A, B](a: Coder[A], b: Coder[B]): Tuple2Coder[A, B] = {
    new Tuple2Coder(a, b)
  }

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): Tuple2Coder[_, _] = {
    of(components.get(0), components.get(1))
  }

  def getInstanceComponents[A, B](tuple: Tuple2[A, B]): java.util.List[java.lang.Object] = {
    Seq(tuple._1.asInstanceOf[AnyRef], tuple._2.asInstanceOf[AnyRef]).toList.asJava
  }
}

