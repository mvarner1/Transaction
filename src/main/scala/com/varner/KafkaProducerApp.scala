package com.varner

import java.util.Properties

import com.varner.streams.demo.AppConstant
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

case class Transaction(
                        customer_id: String,
                        spent: String,
                        purchased: String,
                        zip : String,
                        cardNo : String
                      )

object KafkaProducerApp extends App {

  def getMock () : Transaction = {
    val transaction = Transaction("c_id", "10000", "2000", "1216", "12345")
    transaction
  }

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:29092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = AppConstant.TOPIC_TRANSACTION
  var index = 0
  try {
    while(true) {
      val record = new ProducerRecord[String, String](topic, index.toString, getMock().toString)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())

      index += 1

      Thread.sleep(1000) // wait for 1000 millisecond
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}
