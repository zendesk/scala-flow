package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.{PCollection, TupleTag, TupleTagList}

trait BranchOps {
  implicit class RichBranchCollection[A : Coder](collection: PCollection[A]) {
    def branch(p1: A => Boolean, p2: A => Boolean): (PCollection[A], PCollection[A]) = {
      val tag1 = new TupleTag[A]()
      val tag2 = new TupleTag[A]()

      val parDo = WrapperOps.asParDo { (c: DoFn[A, A]#ProcessContext) =>
        val element = c.element()

        if (p1(element))
          c.output(element)
        else if (p2(element))
          c.sideOutput(tag2, element)
        else throw new Exception(s"no predicates matched element $element")
      }.withOutputTags(tag1, TupleTagList.of(tag2))

      val outputs = collection.apply(parDo)
      val coder = implicitly[Coder[A]]

      (
        outputs.get(tag1).setCoder(coder),
        outputs.get(tag2).setCoder(coder)
      )
    }

    def branch(p1: A => Boolean, p2: A => Boolean, p3: A => Boolean): (PCollection[A], PCollection[A], PCollection[A]) = {
      val tag1 = new TupleTag[A]()
      val tag2 = new TupleTag[A]()
      val tag3 = new TupleTag[A]()

      val parDo = WrapperOps.asParDo { (c: DoFn[A, A]#ProcessContext) =>
        val element = c.element()

        if (p1(element))
          c.output(element)
        else if (p2(element))
          c.sideOutput(tag2, element)
        else if (p3(element))
          c.sideOutput(tag3, element)
        else throw new Exception(s"no predicates matched element $element")
      }.withOutputTags(tag1, TupleTagList.of(tag2).and(tag3))

      val outputs = collection.apply(parDo)
      val coder = implicitly[Coder[A]]

      (
        outputs.get(tag1).setCoder(coder),
        outputs.get(tag2).setCoder(coder),
        outputs.get(tag3).setCoder(coder)
      )
    }
  }
}

object BranchOps extends BranchOps
