package com.zendesk.scalaflow.coders

import java.io.{IOException, InputStream, OutputStream}
import java.util.{Arrays, List => JList}

import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{Coder, CustomCoder}
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

class EitherCoder[A, B](aCoder: Coder[A], bCoder: Coder[B]) extends CustomCoder[Either[A, B]] {

  override def encode(value: Either[A, B], outStream: OutputStream, context: Context): Unit = {
    value match {
      case Left(left) =>
        outStream.write(1)
        aCoder.encode(left, outStream, context.nested)
      case Right(right) =>
        outStream.write(0)
        bCoder.encode(right, outStream, context.nested)
    }
  }

  override def decode(inStream: InputStream, context: Context): Either[A, B] = {
    val tag = inStream.read

    if (tag == 1) Left(aCoder.decode(inStream, context.nested))
    else if (tag == 0) Right(bCoder.decode(inStream, context.nested))
    else throw new IOException(s"Unexpected value $tag encountered decoding 1 byte from input stream")
  }

  override def consistentWithEquals(): Boolean = {
    aCoder.consistentWithEquals && bCoder.consistentWithEquals
  }

  override def getCoderArguments: JList[Coder[_]] = {
    Arrays.asList(aCoder, bCoder)
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

  override def getEncodingId = s"EitherCoder(${aCoder.getEncodingId},${bCoder.getEncodingId})"
}
