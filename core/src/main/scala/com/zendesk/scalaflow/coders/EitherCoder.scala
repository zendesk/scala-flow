package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}
import java.util

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, StandardCoder}
import com.google.cloud.dataflow.sdk.util.Structs.addBoolean
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver
import com.google.cloud.dataflow.sdk.util.{CloudObject, PropertyNames}

import scala.collection.JavaConverters._

case class EitherCoder[A, B](aCoder: Coder[A], bCoder: Coder[B]) extends StandardCoder[Either[A, B]] {
  private val byteCoder = ByteCoder.of

  override def encode(value: Either[A, B], outStream: OutputStream, context: Context): Unit = {
    val nestedContext = context.nested

    value match {
      case Left(left) =>
        byteCoder.encode(1.toByte, outStream, nestedContext)
        aCoder.encode(left, outStream, nestedContext)
      case Right(right) =>
        byteCoder.encode(0.toByte, outStream, nestedContext)
        bCoder.encode(right, outStream, nestedContext)
    }
  }

  override def decode(inStream: InputStream, context: Context): Either[A, B] = {
    val nestedContext = context.nested

    val tag = byteCoder.decode(inStream, nestedContext)

    if (tag == 1.toByte)
      Left(aCoder.decode(inStream, nestedContext))
    else
      Right(bCoder.decode(inStream, nestedContext))
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

  override def registerByteSizeObserver(value: Either[A, B], observer: ElementByteSizeObserver, context: Context): Unit = {
    value match {
      case Left(left) => aCoder.registerByteSizeObserver(left, observer, context.nested)
      case Right(right) => bCoder.registerByteSizeObserver(right, observer, context.nested)
    }
  }

  override def structuralValue(value: Either[A, B]): AnyRef = {
    if (consistentWithEquals)
      value
    else
      value match {
        case Left(left) => aCoder.structuralValue(left)
        case Right(right) => bCoder.structuralValue(right)
      }
  }

  override def isRegisterByteSizeObserverCheap(value: Either[A, B], context: Context): Boolean = {
    value match {
      case Left(left) => aCoder.isRegisterByteSizeObserverCheap(left, context.nested)
      case Right(right) => bCoder.isRegisterByteSizeObserverCheap(right, context.nested)
    }
  }

  override def asCloudObject(): CloudObject = {
    val result = super.asCloudObject
    addBoolean(result, PropertyNames.IS_PAIR_LIKE, true)
    result
  }

  override def getEncodingId = "EitherCoder"
}

object EitherCoder {
  def of[A, B](a: Coder[A], b: Coder[B]): EitherCoder[A, B] = {
    new EitherCoder(a, b)
  }

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): EitherCoder[_, _] = {
    of(components.get(0), components.get(1))
  }

  def getInstanceComponents[A, B](value: Either[A, B]): java.util.List[java.lang.Object] = {
    value match {
      case Left(left) => Seq(left.asInstanceOf[AnyRef]).toList.asJava
      case Right(right) => Seq(right.asInstanceOf[AnyRef]).toList.asJava
    }
  }
}

