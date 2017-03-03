package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.{ListCoder => _, SetCoder => _, _}
import com.google.cloud.dataflow.sdk.values.KV
import com.zendesk.scalaflow.coders._
import org.joda.time.Instant

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

trait CoderOps {
  implicit val intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]

  implicit val longCoder: Coder[Long] = VarLongCoder.of().asInstanceOf[Coder[Long]]

  implicit val doubleCoder: Coder[Double] = DoubleCoder.of().asInstanceOf[Coder[Double]]

  implicit val stringCoder: Coder[String] = StringUtf8Coder.of

  implicit def listCoder[T](implicit t: Coder[T]): Coder[List[T]] = ListCoder.of(t)

  implicit def setCoder[T](implicit t: Coder[T]): Coder[Set[T]] = SetCoder.of(t)

  implicit def optionCoder[T](implicit t: Coder[T]): Coder[Option[T]] = OptionCoder.of(t)

  implicit def tryCoder[T](implicit t: Coder[T]): Coder[Try[T]] = TryCoder.of(t)

  implicit def eitherCoder[A, B](implicit a: Coder[A], b: Coder[B]): Coder[Either[A, B]] = EitherCoder.of(a, b)

  implicit def kvCoder[A, B](implicit a: Coder[A], b: Coder[B]): Coder[KV[A, B]] = KvCoder.of(a, b)

  implicit val instantCoder: Coder[Instant] = InstantCoder.of()

  implicit def serializableCoder[T <: Serializable](implicit tag: TypeTag[T]): Coder[T] = {
    val clazz = tag.mirror.runtimeClass(tag.tpe.dealias).asInstanceOf[Class[T]]
    SerializableCoder.of(clazz)
  }
}

object CoderOps extends CoderOps
