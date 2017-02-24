package com.zendesk.scalaflow.coders

import java.util.{Set => JavaSet}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.cloud.dataflow.sdk.coders.{Coder, DelegateCoder, SetCoder => JavaSetCoder}
import com.google.cloud.dataflow.sdk.util.PropertyNames

import scala.collection.JavaConverters._

case class SetCoder[T](coder: Coder[T]) extends DelegateCoder[Set[T], JavaSet[T]](JavaSetCoder.of(coder), set => set.asJava, javaSet => Set(javaSet.asScala.toList: _*))

object SetCoder {
  def of[T](coder: Coder[T]): SetCoder[T] = SetCoder(coder)

  @JsonCreator
  def of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) components: java.util.List[Coder[_]]): SetCoder[_] = {
    of(components.get(0))
  }

  def getInstanceComponents[T](set: Set[T]): java.util.List[java.lang.Object] = {
    set.map(_.asInstanceOf[AnyRef]).toList.asJava
  }
}
