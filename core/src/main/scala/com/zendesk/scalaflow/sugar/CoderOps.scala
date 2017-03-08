package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.DelegateCoder.CodingFunction
import com.google.cloud.dataflow.sdk.coders.{ListCoder => JavaListCoder, SetCoder => JavaSetCoder, _}
import com.google.cloud.dataflow.sdk.values.KV
import com.zendesk.scalaflow.coders._
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

trait CoderOps {
  def delegateCoder[A, B](convert: (A) => B, invert: (B) => A)(implicit bCoder: Coder[B]): Coder[A] = {
    val convertFn = new CodingFunction[A, B] {
      override def apply(x: A): B = convert(x)
    }

    val invertFn = new CodingFunction[B, A] {
      override def apply(x: B): A = invert(x)
    }

    DelegateCoder.of[A, B](bCoder, convertFn, invertFn)
  }

  implicit val intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]

  implicit val longCoder: Coder[Long] = VarLongCoder.of().asInstanceOf[Coder[Long]]

  implicit val doubleCoder: Coder[Double] = DoubleCoder.of().asInstanceOf[Coder[Double]]

  implicit val stringCoder: Coder[String] = StringUtf8Coder.of

  implicit def javaListCoder[T](implicit t: Coder[T]): Coder[java.util.List[T]] = JavaListCoder.of(t)

  implicit def javaSetCoder[T](implicit t: Coder[T]): Coder[java.util.Set[T]] = JavaSetCoder.of(t)

  implicit def javaMapCoder[K, V](implicit k: Coder[K], v: Coder[V]): Coder[java.util.Map[K, V]] = MapCoder.of(k, v)

  implicit def listCoder[T](implicit t: Coder[T]): Coder[List[T]] = {
    delegateCoder[List[T], java.util.List[T]](_.asJava, _.asScala.toList)
  }

  implicit def setCoder[T](implicit t: Coder[T]): Coder[Set[T]] = {
    delegateCoder[Set[T], java.util.Set[T]](_.asJava, javaSet => javaSet.asScala.toSet)
  }

  implicit def mapCoder[K, V](implicit k: Coder[K], v: Coder[V]): Coder[Map[K, V]] = {
    delegateCoder[Map[K, V], java.util.Map[K, V]](_.asJava, x => x.asScala.toMap)
  }

  implicit def optionCoder[T](implicit t: Coder[T]): Coder[Option[T]] = OptionCoder.of(t)

  implicit def tryCoder[T](implicit t: Coder[T]): Coder[Try[T]] = TryCoder.of(t)

  implicit def eitherCoder[A, B](implicit a: Coder[A], b: Coder[B]): Coder[Either[A, B]] = EitherCoder.of(a, b)

  implicit def kvCoder[A, B](implicit a: Coder[A], b: Coder[B]): Coder[KV[A, B]] = KvCoder.of(a, b)

  implicit val instantCoder: Coder[Instant] = InstantCoder.of()

  implicit def iterableCoder[T](implicit t: Coder[T]): Coder[java.lang.Iterable[T]] = IterableCoder.of(t)

  implicit def serializableCoder[T <: Serializable](implicit tag: TypeTag[T]): Coder[T] = {
    val clazz = tag.mirror.runtimeClass(tag.tpe.dealias).asInstanceOf[Class[T]]
    SerializableCoder.of(clazz)
  }
}

object CoderOps extends CoderOps
