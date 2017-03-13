package com.zendesk.scalaflow.coders

import java.io.{IOException, InputStream, OutputStream}
import java.util.{Arrays, List => JList}

import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

import scala.util.{Failure, Success, Try}

class TryCoder[A](coder: Coder[A]) extends CustomCoder[Try[A]] {

  private val errorCoder = SerializableCoder.of(classOf[Throwable])

  override def encode(value: Try[A], outStream: OutputStream, context: Context): Unit = {
    value match {
      case Failure(failure) =>
        outStream.write(1)
        errorCoder.encode(failure, outStream, context.nested)
      case Success(success) =>
        outStream.write(0)
        coder.encode(success, outStream, context.nested)
    }
  }

  override def decode(inStream: InputStream, context: Context): Try[A] = {
    val tag = inStream.read()

    if (tag == 1) Failure(errorCoder.decode(inStream, context.nested))
    else if (tag == 0) Success(coder.decode(inStream, context.nested))
    else throw new IOException(s"Unexpected value $tag encountered decoding 1 byte from input stream")
  }

  override def consistentWithEquals(): Boolean = false

  override def getCoderArguments: JList[Coder[_]] = Arrays.asList(coder)

  override def verifyDeterministic(): Unit = {
    throw new Coder.NonDeterministicException(this, "Java Serialization may be non-deterministic.")
  }

  override def registerByteSizeObserver(value: Try[A], observer: ElementByteSizeObserver, context: Context): Unit = {
    value match {
      case Failure(failure) => errorCoder.registerByteSizeObserver(failure, observer, context.nested)
      case Success(success) => coder.registerByteSizeObserver(success, observer, context.nested)
    }
  }

  override def structuralValue(value: Try[A]): AnyRef = {
    if (consistentWithEquals)
      value
    else
      value match {
        case Failure(failure) => errorCoder.structuralValue(failure)
        case Success(success) => coder.structuralValue(success)
      }
  }

  override def isRegisterByteSizeObserverCheap(value: Try[A], context: Context): Boolean = {
    value match {
      case Failure(failure) => errorCoder.isRegisterByteSizeObserverCheap(failure, context.nested)
      case Success(success) => coder.isRegisterByteSizeObserverCheap(success, context.nested)
    }
  }

  override def getEncodingId = s"TryCoder(${coder.getEncodingId})"
}
