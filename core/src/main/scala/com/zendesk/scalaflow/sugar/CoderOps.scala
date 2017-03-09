package com.zendesk.scalaflow.sugar

import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap, Set => JSet}

import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.values.KV
import com.zendesk.scalaflow.coders._
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

trait CoderOps {

  // Built in coders
  implicit val stringCoder: Coder[String] = StringUtf8Coder.of()

  implicit val instantCoder: Coder[Instant] = InstantCoder.of()

  implicit def kvCoder[K, V](implicit k: Coder[K], v: Coder[V]): Coder[KV[K, V]] = KvCoder.of(k, v)

  implicit def javaIterableCoder[T](implicit c: Coder[T]) = IterableCoder.of(c)

  implicit def javaListCoder[T](implicit c: Coder[T]) = ListCoder.of(c)

  implicit def javaMapCoder[K, V](implicit k: Coder[K], v: Coder[V]) = MapCoder.of(k, v)

  implicit def javaSetCoder[T](implicit c: Coder[T]) = SetCoder.of(c)

  // Primitives
  implicit val unitCoder: Coder[Unit] = DelegateCoder.of[Unit, Void](VoidCoder.of(), null, _ => ())

  implicit val intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]

  implicit val longCoder: Coder[Long] = VarLongCoder.of().asInstanceOf[Coder[Long]]

  implicit val doubleCoder: Coder[Double] = DoubleCoder.of().asInstanceOf[Coder[Double]]

  // Core Types
  implicit def optionCoder[T](implicit c: Coder[T]): Coder[Option[T]] = new OptionCoder(c)

  implicit def tryCoder[T](implicit c: Coder[T]): Coder[Try[T]] = new TryCoder(c)

  implicit def eitherCoder[A, B](implicit a: Coder[A], b: Coder[B]): Coder[Either[A, B]] = new EitherCoder(a, b)

  // Immutable Collections
  implicit def iterableCoder[T](implicit c: Coder[T]): Coder[Iterable[T]] = {
    DelegateCoder.of[Iterable[T], JIterable[T]](javaIterableCoder, _.asJava, _.asScala)
  }

  implicit def listCoder[T](implicit c: Coder[T]): Coder[List[T]] = {
    DelegateCoder.of[List[T], JList[T]](javaListCoder, _.asJava, _.asScala.toList)
  }

  implicit def mapCoder[K, V](implicit k: Coder[K], v: Coder[V]): Coder[Map[K, V]] = {
    DelegateCoder.of[Map[K, V], JMap[K, V]](javaMapCoder, _.asJava, _.asScala.toMap)
  }

  implicit def setCoder[T](implicit c: Coder[T]): Coder[Set[T]] = {
    DelegateCoder.of[Set[T], JSet[T]](javaSetCoder, _.asJava, _.asScala.toSet)
  }

  implicit def arrayCoder[T](implicit c: Coder[T], tag: ClassTag[T]): Coder[Array[T]] = {
    DelegateCoder.of[Array[T], JList[T]](javaListCoder, _.toList.asJava, _.asScala.toArray)
  }

  // Opt-in convienience catch-all coder for anything that doesn't fit in above and is not a case class
  def serializableCoder[T <: Serializable](implicit tag: ClassTag[T]): Coder[T] = {
    SerializableCoder.of(tag.runtimeClass.asInstanceOf[Class[T]])
  }
}

object CoderOps extends CoderOps
