package com.zendesk.scalaflow.sugar

import com.google.cloud.bigtable.dataflow.CloudBigtableIO
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.{Coder, CoderFactory, DoubleCoder, VarIntCoder, VarLongCoder}
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput}
import com.zendesk.scalaflow.coders._

import scala.util.Try
import scala.reflect.runtime.universe._
import WrapperOps._

trait PipelineOps {

  implicit class RichPipeline(pipeline: Pipeline) {
    val coderRegistry = pipeline.getCoderRegistry()

    def registerScalaCoders(): Pipeline = {
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

    def registerCoder(clazz: Class[_], coderClazz: Class[_]): Pipeline = {
      coderRegistry.registerCoder(clazz, coderClazz)
      pipeline
    }

    def registerCoder(clazz: Class[_], coderFactory: CoderFactory): Pipeline = {
      coderRegistry.registerCoder(clazz, coderFactory)
      pipeline
    }

    def registerCoder[T](rawClazz: Class[T], coder: Coder[T]): Pipeline = {
      coderRegistry.registerCoder(rawClazz, coder)
      pipeline
    }

    def initializeForBigTableWrite(): Pipeline = {
      CloudBigtableIO.initializeForWrite(pipeline)
      pipeline
    }
  }

  implicit class RichBegin(begin: PBegin) {
    import CollectionOps.RichCollection

    def transformWith[A <: POutput](name: String)(f: PBegin => A) = {
      begin.apply(name, asPTransform(f))
    }

    def flatten[A: TypeTag](first: PCollection[A], second: PCollection[A], others: PCollection[A]*): PCollection[A] = {
      first.flattenWith(second, others: _*)
    }
  }

  implicit class RichOutput(output: POutput) {
    def run() = output.getPipeline.run()
  }
}

object PipelineOps extends PipelineOps
