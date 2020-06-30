package com.varner.streams.demo.app.data.generator

import java.util.regex.Pattern
import java.util.{Date, Properties}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.varner.streams.demo.model.Purchase
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.github.javafaker.{Faker}
import com.varner.streams.demo.AppConstant
import util.control.Breaks._


object KafkaPurchaseProducerApp extends App {
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  val properties = new Properties
  properties.put("bootstrap.servers", "localhost:29092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("acks", "1")
  properties.put("retries", "3")

  val faker = new Faker
  val visaMasterCardAmex = Pattern.compile("(\\d{4}-){3}\\d{4}")
  def getMockPurchaseAsJson () : String = {
    val name = faker.name().name()
    val customerId = faker.idNumber.valid()
    val creditCardNumber= buildFakeCreditCardNumber
    val itemPurchased = faker.commerce.productName
    val quantity = faker.number.numberBetween(1, 5)
    val price = faker.commerce.price(1.00, 100.00).toDouble;
    val purchaseDate = new Date()
    val zipCode = faker.options.option("1111", "2222", "3333", "4444");

    val purchase = Purchase(name, customerId,creditCardNumber, itemPurchased, quantity, price,purchaseDate, zipCode)

    val json = mapper.writeValueAsString(purchase)
    json
  }
  def buildFakeCreditCardNumber() : String = {
    var creditCardNumber = ""
    breakable {
      while (true) {
        val finance = faker.finance
        val cardNumber = finance.creditCard
        if (visaMasterCardAmex.matcher(cardNumber).matches) {
          creditCardNumber = cardNumber
          break
        }
      }
    }

    creditCardNumber

  }

  val producer = new KafkaProducer[String, String](properties)
  val topic = AppConstant.TOPIC_TRANSACTION
  var index = 0
  val noOfRecords = 1
  try {
    for (index <- 0 until noOfRecords) {

      val purchaseAsJson  = getMockPurchaseAsJson()
      val record = new ProducerRecord[String, String](topic, index.toString, purchaseAsJson)
      val metadata = producer.send(record)
      println(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())

      Thread.sleep(1000) // wait for 1000 millisecond
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}
