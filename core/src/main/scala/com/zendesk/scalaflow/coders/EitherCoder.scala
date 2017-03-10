package com.zendesk.scalaflow.coders

import java.io.{InputStream, OutputStream}
import java.util.{List => JList}

import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{ByteCoder, Coder, CustomCoder}
import com.google.cloud.dataflow.sdk.util.Structs.addBoolean
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver
import com.google.cloud.dataflow.sdk.util.{CloudObject, PropertyNames}

class EitherCoder[A, B](aCoder: Coder[A], bCoder: Coder[B]) extends CustomCoder[Either[A, B]] {
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

  override def consistentWithEquals(): Boolean = {
    aCoder.consistentWithEquals && bCoder.consistentWithEquals
  }

  override def getCoderArguments: JList[Coder[_]] = {
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
