package com.zendesk.scalaflow.sugar

import java.lang.reflect.{GenericArrayType, ParameterizedType, Type => JType}
import java.lang.{Iterable => JIterable}

import com.google.cloud.bigtable.dataflow.CloudBigtableIO
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.{DoubleCoder, VarIntCoder, VarLongCoder}
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values._
import com.zendesk.scalaflow.coders._
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.Try

object Implicits {

  implicit class IntWithTimeMethods(int: Int) {
    def second = Duration.standardSeconds(int)
    def seconds = Duration.standardSeconds(int)

    def minute = Duration.standardMinutes(int)
    def minutes = Duration.standardMinutes(int)

    def hour = Duration.standardHours(int)
    def hours = Duration.standardHours(int)

    def day = Duration.standardDays(int)
    def days = Duration.standardDays(int)
  }

  implicit class RichPipeline(pipeline: Pipeline) {
    val coderRegistry = pipeline.getCoderRegistry()

    def registerScalaCoders() = {
      coderRegistry.registerCoder(classOf[Int], classOf[VarIntCoder])
      coderRegistry.registerCoder(classOf[Long], classOf[VarLongCoder])
      coderRegistry.registerCoder(classOf[Double], classOf[DoubleCoder])
      coderRegistry.registerCoder(classOf[List[_]], classOf[ListCoder[_]])
      coderRegistry.registerCoder(classOf[Set[_]], classOf[SetCoder[_]])
      coderRegistry.registerCoder(classOf[Option[_]], classOf[OptionCoder[_]])
      coderRegistry.registerCoder(classOf[Try[_]], classOf[TryCoder[_]])
      coderRegistry.registerCoder(classOf[Either[_, _]], classOf[EitherCoder[_, _]])
      TupleCoders.register(coderRegistry)
      pipeline
    }

    def initializeForBigTableWrite() = {
      CloudBigtableIO.initializeForWrite(pipeline)
      pipeline
    }
  }

  implicit class RichTypeTag[X](tag: TypeTag[X]) {
    import definitions._

    val asTypeDescriptor: TypeDescriptor[X] = {
      TypeDescriptor.of(convert(tag.tpe)).asInstanceOf[TypeDescriptor[X]]
    }

    private def convert(tpe: Type): JType = {
      // Make sure to get the raw underlying types,
      // in case tpe is an alias e.g type Foo = List[Int]
      val expanded = tpe.dealias
      val clazz = tag.mirror.runtimeClass(expanded)

      if (clazz.isPrimitive) convertPrimitive(expanded, clazz)
      else if (clazz.isArray) convertArray(expanded, clazz)
      else convertObject(expanded, clazz)
    }

    private def convertPrimitive(tpe: Type, clazz: Class[_]) = tpe match {
      case CharTpe => classOf[java.lang.Character]
      case ByteTpe => classOf[java.lang.Byte]
      case ShortTpe => classOf[java.lang.Short]
      case IntTpe => classOf[java.lang.Integer]
      case LongTpe => classOf[java.lang.Long]
      case FloatTpe => classOf[java.lang.Float]
      case DoubleTpe => classOf[java.lang.Double]
      case _ => throw new IllegalArgumentException(s"Unsupported primitive type $clazz")
    }

    private def convertArray[T](tpe: Type, clazz: Class[_]) = {
      val componentType = tpe.typeArgs.head

      if (componentType.typeArgs.isEmpty) {
        clazz
      } else new GenericArrayType {
        override def getGenericComponentType = convert(componentType)
      }
    }

    private def convertObject(tpe: Type, clazz: Class[_]) = {
      if (tpe.typeArgs.isEmpty) {
        clazz
      } else new ParameterizedType {
        override def getOwnerType = null
        override def getRawType = clazz
        override def getActualTypeArguments = tpe.typeArgs.map(convert).toArray
      }
    }
  }

  implicit class RichBegin(input: PBegin) {
    def transformWith[OutputT <: POutput](name: String)(f: PBegin => OutputT): OutputT = {
      input.apply(name, new PTransform[PBegin, OutputT] {
        override def apply(x: PBegin): OutputT = f(x)
      })
    }
  }

  implicit class RichOutput(output: POutput) {
    def run() = output.getPipeline.run()
  }

  implicit class RichCollection[A: TypeTag](val collection: PCollection[A]) {
    def parDo[B: TypeTag](f: DoFn[A, B]#ProcessContext => Unit): PCollection[B] = {
      collection.apply(asParDo(f))
    }

    def map[B: TypeTag](f: A => B): PCollection[B] = parDo {
      c => c.output(f(c.element))
    }

    def filter(f: A => Boolean): PCollection[A] = parDo {
      c => if (f(c.element)) c.output(c.element)
    }

    def collect[B: TypeTag](pf: PartialFunction[A, B]): PCollection[B] = parDo {
      c => if (pf.isDefinedAt(c.element)) c.output(pf(c.element))
    }

    def extractTimestamp: PCollection[(A, Instant)] = parDo {
      c => c.output((c.element, c.timestamp))
    }

    def flatMap[B: TypeTag](f: A => Iterable[B]): PCollection[B] = parDo {
      c => f(c.element).foreach(c.output)
    }

    def foreach(f: A => Unit): PCollection[A] = parDo {
      c => { f(c.element); c.output(c.element) }
    }

    def withKey[B: TypeTag](f: A => B): PCollection[KV[B, A]] = parDo {
      c => c.output(KV.of(f(c.element), c.element))
    }

    def flattenWith(first: PCollection[A], others: PCollection[A]*): PCollection[A] = {
      val all  = collection :: first :: others.toList
      PCollectionList.of(all.asJava).apply(Flatten.pCollections[A])
    }

    def transformWith[OutputT <: POutput](name: String)(f: PCollection[A] => OutputT): OutputT = {
      collection.apply(name, new PTransform[PCollection[A], OutputT] {
        override def apply(x: PCollection[A]): OutputT = f(x)
      })
    }
  }

  implicit class RichStringCollection(val collection: PCollection[String]) {
    def writeToPubsub(topic: String): PDone = {
      collection.apply(PubsubIO.Write.topic(topic))
    }
  }

  implicit class RichTryCollection[A: TypeTag](val collection: PCollection[Try[A]]) {
    /**
      * Map only success values, keeping failures intact.
      */
    def mapSuccess[B](f: A => B)(implicit tag: TypeTag[Try[B]]): PCollection[Try[B]] = {
      val g = (c: DoFn[Try[A], Try[B]]#ProcessContext) => c.output(c.element.map(f))
      collection.apply(asParDo(g))
    }

    def flatMapSuccess[B](f: A => Try[B])(implicit tag: TypeTag[Try[B]]): PCollection[Try[B]] = {
      val g = (c: DoFn[Try[A], Try[B]]#ProcessContext) => c.output(c.element.flatMap(f))
      collection.apply(asParDo(g))
    }
  }

  implicit class RichKVCollection[K: TypeTag, A: TypeTag](val collection: PCollection[KV[K, A]]) {

    def parDo[B: TypeTag](f: DoFn[KV[K, A], KV[K, B]]#ProcessContext => Unit): PCollection[KV[K, B]] = {
      collection.apply(asParDo(f))
    }

    def mapValue[B: TypeTag](f: A => B): PCollection[KV[K, B]] = parDo {
      c => c.output(KV.of(c.element.getKey, f(c.element.getValue)))
    }

    def flatMapValue[B: TypeTag](f: A => Iterable[B]): PCollection[KV[K, B]] = parDo {
      c => f(c.element.getValue).foreach { value => c.output(KV.of(c.element.getKey, value)) }
    }

    def combinePerKey(zero: A)(f: (A, A) => A): PCollection[KV[K, A]] = {
      val g = (input: JIterable[A]) => input.asScala.fold(zero)(f)
      collection.apply(Combine.perKey[K, A](asSimpleFn(g)))
    }

    def groupByKey: PCollection[KV[K, List[A]]] = {
      collection
        .apply(GroupByKey.create[K, A])
        .mapValue(_.asScala.toList)
    }

    def extractTimestamp: PCollection[KV[K, (A, Instant)]] = parDo {
      c => c.output(KV.of(c.element.getKey, (c.element.getValue, c.timestamp)))
    }
  }

  implicit class RichParDo[A, B](parDo: ParDo.Bound[A, B]) {

    def withMultipleOutput[T](main: TupleTag[B])(sides: TupleTag[_]*): ParDo.BoundMulti[A, B] = {
      parDo.withOutputTags(main, TupleTagList.of(sides.toList.asJava))
    }
  }

  private def asSimpleFn[A, B](f: A => B)(implicit tag: TypeTag[B]): SimpleFunction[A, B] = {
    // The underlying TypeTag code created by the Scala compiler closes around the containing classes,
    // so to ensure the SimpleFunction is serializable "asTypeDescriptor" needs to be called here,
    // then passed into the SimpleFunction as a by-value parameter
    val descriptor = tag.asTypeDescriptor

    new SimpleFunction[A, B] {
      override def apply(input: A): B = f(input)
      override def getOutputTypeDescriptor = descriptor
    }
  }

  def asParDo[A, B](f: DoFn[A, B]#ProcessContext => Unit)(implicit tag: TypeTag[B]): ParDo.Bound[A, B] = {
    // The underlying TypeTag  code created by the Scala compiler closes around the containing classes,
    // so to ensure the DoFn is serializable "asTypeDescriptor" needs to be called here,
    // then passed into the DoFn as a by-value parameter
    val descriptor = tag.asTypeDescriptor

    val doFn = new DoFn[A, B] {
      override def processElement(c: DoFn[A, B]#ProcessContext): Unit = f(c)
      override def getOutputTypeDescriptor = descriptor
    }

    ParDo.of(doFn)
  }
}
