package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput}
import com.zendesk.scalaflow.sugar.CollectionOps.RichCollection
import com.zendesk.scalaflow.sugar.WrapperOps._

trait PipelineOps {

  implicit class RichBegin(begin: PBegin) {

    def transform[T](values: Create.Values[T])(implicit coder: Coder[T]): PCollection[T] = {
      begin.apply(values.withCoder(coder))
    }

    def transformWith[A <: POutput](name: String)(f: PBegin => A) = {
      begin.apply(name, asPTransform(f))
    }

    def flatten[A : Coder](first: PCollection[A], second: PCollection[A], others: PCollection[A]*): PCollection[A] = {
      first.flattenWith(second, others: _*)
    }
  }

  implicit class RichOutput(output: POutput) {
    def run() = output.getPipeline.run()
  }
}

object PipelineOps extends PipelineOps
