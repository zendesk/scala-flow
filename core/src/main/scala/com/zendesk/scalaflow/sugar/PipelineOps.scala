package com.zendesk.scalaflow.sugar

import com.google.cloud.bigtable.dataflow.CloudBigtableIO
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput}
import com.zendesk.scalaflow.sugar.WrapperOps._

import scala.reflect.runtime.universe._

trait PipelineOps {

  implicit class RichPipeline(pipeline: Pipeline) {
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

    def flatten[A: TypeTag : Coder](first: PCollection[A], second: PCollection[A], others: PCollection[A]*): PCollection[A] = {
      first.flattenWith(second, others: _*)
    }
  }

  implicit class RichOutput(output: POutput) {
    def run() = output.getPipeline.run()
  }
}

object PipelineOps extends PipelineOps
