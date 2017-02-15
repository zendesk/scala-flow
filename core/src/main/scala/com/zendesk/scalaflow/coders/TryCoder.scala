package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}
import java.util

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, SerializableCoder, StandardCoder}
import com.google.cloud.dataflow.sdk.util.PropertyNames
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class TryCoder[A](coder: Coder[A]) extends StandardCoder[Try[A]] {
  private val byteCoder = ByteCoder.of
  private val errorCoder = SerializableCoder.of(classOf[Throwable])

  override def encode(value: Try[A], outStream: OutputStream, context: Context): Unit = {
    val nestedContext = context.nested

    value match {
      case Failure(failure) =>
        byteCoder.encode(1.toByte, outStream, nestedContext)
        errorCoder.encode(failure, outStream, nestedContext)
      case Success(success) =>
        byteCoder.encode(0.toByte, outStream, nestedContext)
        coder.encode(success, outStream, nestedContext)
    }
  }

  override def decode(inStream: InputStream, context: Context): Try[A] = {
    val nestedContext = context.nested

    val tag = byteCoder.decode(inStream, nestedContext)

    if (tag == 1.toByte)
      Failure(errorCoder.decode(inStream, nestedContext))
    else
      Success(coder.decode(inStream, nestedContext))
  }

  override def consistentWithEquals(): Boolean = {
    errorCoder.consistentWithEquals && coder.consistentWithEquals
  }

  override def getCoderArguments: util.List[_ <: Coder[_]] = {
    java.util.Arrays.asList(coder)
  }

  override def verifyDeterministic(): Unit = {
    verifyDeterministic("Error coder must be deterministic", errorCoder)
    verifyDeterministic("Coder must be deterministic", coder)
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

  override def getEncodingId = "TryCoder"
}

object TryCoder {
  def of[A](coder: Coder[A]): TryCoder[A] = {
    new TryCoder(coder)
  }

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): TryCoder[_] = {
    of(components.get(0))
  }

  def getInstanceComponents[A](value: Try[A]): java.util.List[java.lang.Object] = {
    value match {
      case Failure(failure) => Seq(failure.asInstanceOf[AnyRef]).toList.asJava
      case Success(success) => Seq(success.asInstanceOf[AnyRef]).toList.asJava
    }
  }
}

