package com.zendesk.scalaflow.sugar

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.{PCollection, TupleTag, TupleTagList}

import com.zendesk.scalaflow._

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

    def branchMap[B1 : Coder, B2 : Coder](p1: PartialFunction[A, B1], p2: PartialFunction[A, B2]): (PCollection[B1], PCollection[B2]) = {
      val (output1, output2) = branch(p1.isDefinedAt _, p2.isDefinedAt _)

      val coder1 = implicitly[Coder[B1]]
      val coder2 = implicitly[Coder[B2]]

      (
        output1.map(p1).setCoder(coder1),
        output2.map(p2).setCoder(coder2)
      )
    }
  }
}

object BranchOps extends BranchOps
