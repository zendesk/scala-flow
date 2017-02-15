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

case class Tuple3Coder[A, B, C](aCoder: Coder[A], bCoder: Coder[B], cCoder: Coder[C]) extends StandardCoder[Tuple3[A, B, C]] {
  override def encode(value: (A, B, C), outStream: OutputStream, context: Context): Unit = {
    val nestedContext = context.nested

    aCoder.encode(value._1, outStream, nestedContext)
    bCoder.encode(value._2, outStream, nestedContext)
    cCoder.encode(value._3, outStream, nestedContext)
  }

  override def decode(inStream: InputStream, context: Context): (A, B, C) = {
    val nestedContext = context.nested
    val a = aCoder.decode(inStream, nestedContext)
    val b = bCoder.decode(inStream, nestedContext)
    val c = cCoder.decode(inStream, nestedContext)

    (a, b, c)
  }

  def getCoder1(): Coder[A] = aCoder
  def getCoder2(): Coder[B] = bCoder
  def getCoder3(): Coder[C] = cCoder

  override def consistentWithEquals(): Boolean = {
    aCoder.consistentWithEquals &&
      bCoder.consistentWithEquals &&
      cCoder.consistentWithEquals
  }

  override def getCoderArguments: util.List[_ <: Coder[_]] = {
    java.util.Arrays.asList(aCoder, bCoder, cCoder)
  }

  override def verifyDeterministic(): Unit = {
    verifyDeterministic("First coder must be deterministic", aCoder)
    verifyDeterministic("Second coder must be deterministic", bCoder)
    verifyDeterministic("Second coder must be deterministic", cCoder)
  }

  override def registerByteSizeObserver(value: (A, B, C), observer: ElementByteSizeObserver, context: Context): Unit = {
    aCoder.registerByteSizeObserver(value._1, observer, context.nested)
    bCoder.registerByteSizeObserver(value._2, observer, context.nested)
    cCoder.registerByteSizeObserver(value._3, observer, context.nested)
  }

  override def structuralValue(value: (A, B, C)): AnyRef = {
    if (consistentWithEquals)
      value
    else
      (aCoder.structuralValue(value._1), bCoder.structuralValue(value._2), cCoder.structuralValue(value._3))
  }

  override def isRegisterByteSizeObserverCheap(value: (A, B, C), context: Context): Boolean = {
    aCoder.isRegisterByteSizeObserverCheap(value._1, context.nested) &&
      bCoder.isRegisterByteSizeObserverCheap(value._2, context.nested) &&
      cCoder.isRegisterByteSizeObserverCheap(value._3, context.nested)
  }

  override def asCloudObject(): CloudObject = {
    val result = super.asCloudObject
    addBoolean(result, PropertyNames.IS_PAIR_LIKE, true)
    result
  }

  override def getEncodingId = "Tuple3Coder"
}

object Tuple3Coder {
  def of[A, B, C](a: Coder[A], b: Coder[B], c: Coder[C]): Tuple3Coder[A, B, C] = {
    new Tuple3Coder(a, b, c)
  }

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): Tuple3Coder[_, _, _] = {
    of(components.get(0), components.get(1), components.get(2))
  }

  def getInstanceComponents[A, B, C](tuple: Tuple3[A, B, C]): java.util.List[java.lang.Object] = {
    Seq(tuple._1.asInstanceOf[AnyRef], tuple._2.asInstanceOf[AnyRef], tuple._3.asInstanceOf[AnyRef]).toList.asJava
  }
}

