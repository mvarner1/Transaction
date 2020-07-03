package com.varner.streams.demo.util


import java.util
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import com.fasterxml.jackson.databind.exc.{UnrecognizedPropertyException => UPE}
import scala.reflect.ClassTag

class CustomObjectSerde[T >: Null : ClassTag] extends Serde[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def deserializer(): Deserializer[T] = new CustomObjectDeserializer[T]
  override def serializer(): Serializer[T] = new CustomObjectSerializer[T]
}

class CustomObjectSerializer[T >: Null : ClassTag] extends Serializer[T] {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def serialize(topic: String, data: T): Array[Byte] = {
    CustomObjectSerdeHelper.ByteArray.encode(data)
  }
}
class CustomObjectDeserializer[T >: Null : ClassTag] extends Deserializer[T]  {
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
  override def close(): Unit = ()
  override def deserialize(topic: String, data: Array[Byte]): T ={
    CustomObjectSerdeHelper.ByteArray.decode[T](data)
  }
}

object CustomObjectSerdeHelper {

  type ParseException = JsonParseException
  type UnrecognizedPropertyException = UPE

  private val mapper = new ObjectMapper()

  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  object ByteArray {
    def encode(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)

    def decode[T >: Null : ClassTag](data: Array[Byte]): T =  data match {
      case null => null
      case _ =>
        try {
          mapper.readValue(data, scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
        } catch {
          case e: Exception =>
            val jsonStr = new String(data, "UTF-8")
            println("Error occurred while decoding JSON: "+ jsonStr + "Error: " , e)
            null
        }

    }

  }
}
