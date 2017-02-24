package com.zendesk.scalaflow.coders

import java.util.{List => JavaList}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.{Coder, DelegateCoder, ListCoder => JavaListCoder}
import com.google.cloud.dataflow.sdk.util.PropertyNames

import scala.collection.JavaConverters._

case class ListCoder[T](coder: Coder[T]) extends DelegateCoder[List[T], JavaList[T]](JavaListCoder.of(coder), list => list.asJava, javaList => javaList.asScala.toList)

object ListCoder {
  def of[T](coder: Coder[T]): ListCoder[T] = ListCoder(coder)

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): ListCoder[_] = {
    of(components.get(0))
  }

  def getInstanceComponents[T](list: List[T]): java.util.List[java.lang.Object] = {
    list.map(_.asInstanceOf[AnyRef]).asJava
  }
}

