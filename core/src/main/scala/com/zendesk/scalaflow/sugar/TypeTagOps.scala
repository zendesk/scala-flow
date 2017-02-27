package com.zendesk.scalaflow.sugar

import java.lang.reflect.{GenericArrayType, ParameterizedType, Type => JType}

import com.google.cloud.dataflow.sdk.values.TypeDescriptor

import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.definitions._

trait TypeTagOps {

  implicit class RichTypeTag[X](tag: TypeTag[X]) {

    val asTypeDescriptor = TypeDescriptor.of(convert(tag.tpe)).asInstanceOf[TypeDescriptor[X]]

    private def convert(tpe: Type): JType = {
      // Make sure to get the raw underlying types, in case tpe is an alias e.g type Foo = List[Int]
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
}

object TypeTagOps extends TypeTagOps
