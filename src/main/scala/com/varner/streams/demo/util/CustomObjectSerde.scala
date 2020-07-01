package com.varner.streams.demo.util

import java.lang.reflect.{ParameterizedType, Type}
import java.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import com.fasterxml.jackson.databind.exc.{UnrecognizedPropertyException => UPE}

class CustomObjectSerde[T >: Null <: Any : Manifest] extends Serde[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def deserializer(): Deserializer[T] = new CustomObjectDeserializer[T]
  override def serializer(): Serializer[T] = new CustomObjectSerializer[T]
}

class CustomObjectSerializer[T >: Null <: Any : Manifest] extends Serializer[T] {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def serialize(topic: String, data: T): Array[Byte] = {
    CustomObjectSerdeHelper.ByteArray.encode(data)
  }
}
class CustomObjectDeserializer[T >: Null <: Any : Manifest] extends Deserializer[T]  {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T = {
    CustomObjectSerdeHelper.ByteArray.decode(data)
  }
}

object CustomObjectSerdeHelper {

  type ParseException = JsonParseException
  type UnrecognizedPropertyException = UPE

  private val mapper = new ObjectMapper()

  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  private def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    }
    else new ParameterizedType {
      def getRawType = m.runtimeClass

      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

      def getOwnerType = null
    }
  }

  object ByteArray {
    def encode(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)

    def decode[T: Manifest](value: Array[Byte]): T =
      mapper.readValue(value, typeReference[T])
  }

}
