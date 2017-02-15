package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, StandardCoder}
import com.google.cloud.dataflow.sdk.util.PropertyNames
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

import scala.collection.JavaConverters._

case class OptionCoder[T](valueCoder: Coder[T]) extends StandardCoder[Option[T]] {
  private val byteCoder = ByteCoder.of

  override def encode(value: Option[T], outStream: OutputStream, context: Context): Unit = {
    val nestedContext = context.nested

    value match {
      case Some(left) =>
        byteCoder.encode(1.toByte, outStream, nestedContext)
        valueCoder.encode(left, outStream, nestedContext)
      case None =>
        byteCoder.encode(0.toByte, outStream, nestedContext)
    }
  }

  override def decode(inStream: InputStream, context: Context): Option[T] = {
    val nestedContext = context.nested

    val tag = byteCoder.decode(inStream, nestedContext)

    if (tag == 1.toByte)
      Some(valueCoder.decode(inStream, nestedContext))
    else
      None
  }

  override def consistentWithEquals(): Boolean = {
    valueCoder.consistentWithEquals
  }

  override def getCoderArguments: java.util.List[_ <: Coder[_]] = {
    java.util.Arrays.asList(valueCoder)
  }

  override def verifyDeterministic(): Unit = {
    verifyDeterministic("First coder must be deterministic", valueCoder)
  }

  override def registerByteSizeObserver(value: Option[T], observer: ElementByteSizeObserver, context: Context): Unit = {
    value.foreach(v => valueCoder.registerByteSizeObserver(v, observer, context.nested))
  }

  override def structuralValue(value: Option[T]): AnyRef = {
    if (consistentWithEquals)
      value
    else
      value.map(v => valueCoder.structuralValue(v))
  }

  override def isRegisterByteSizeObserverCheap(value: Option[T], context: Context): Boolean = {
    value
      .map(v => valueCoder.isRegisterByteSizeObserverCheap(v, context.nested))
      .getOrElse(true)
  }

  override def getEncodingId = "OptionCoder"
}

object OptionCoder {
  def of[T](valueCoder: Coder[T]): OptionCoder[T] = {
    new OptionCoder(valueCoder)
  }

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): OptionCoder[_] = {
    of(components.get(0))
  }

  def getInstanceComponents[T](value: Option[T]): java.util.List[java.lang.Object] = {
    value match {
      case Some(v) => Seq(v.asInstanceOf[AnyRef]).toList.asJava
      case None => Seq().toList.asJava
    }
  }
}
