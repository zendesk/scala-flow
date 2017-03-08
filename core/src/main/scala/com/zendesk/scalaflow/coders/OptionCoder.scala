package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}
import java.util.{List => JList}

import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, CustomCoder}
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

class OptionCoder[T](valueCoder: Coder[T]) extends CustomCoder[Option[T]] {
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

  override def getCoderArguments: JList[Coder[_]] = {
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
