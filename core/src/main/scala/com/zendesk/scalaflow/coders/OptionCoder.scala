package com.zendesk.scalaflow.coders

import java.io.{IOException, InputStream, OutputStream}
import java.util.{Arrays, List => JList}

import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, CustomCoder}
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

class OptionCoder[T](valueCoder: Coder[T]) extends CustomCoder[Option[T]] {
  private val byteCoder = ByteCoder.of

  override def encode(value: Option[T], outStream: OutputStream, context: Context): Unit = {
    value match {
      case Some(left) =>
        outStream.write(1)
        valueCoder.encode(left, outStream, context.nested)
      case None =>
        outStream.write(0)
    }
  }

  override def decode(inStream: InputStream, context: Context): Option[T] = {
    val tag = inStream.read()

    if (tag == 1) Some(valueCoder.decode(inStream, context.nested))
    else if (tag == 0) None
    else throw new IOException(s"Unexpected value $tag encountered decoding 1 byte from input stream")
  }

  override def consistentWithEquals(): Boolean = {
    valueCoder.consistentWithEquals
  }

  override def getCoderArguments: JList[Coder[_]] = {
    Arrays.asList(valueCoder)
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

  override def getEncodingId = s"OptionCoder(${valueCoder.getEncodingId})"
}
