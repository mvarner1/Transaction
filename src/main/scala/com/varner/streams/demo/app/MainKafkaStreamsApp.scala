package com.varner.streams.demo.app

package kafkastreams.demo

import com.github.javafaker.Faker
import com.varner.streams.demo.AppConstant
import com.varner.streams.demo.model.{Purchase, Reward}
import com.varner.streams.demo.util.CustomObjectSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.{StreamsBuilder}

object MainKafkaStreamsApp extends App {
  new MainKafkaStreamsApp().start("main-kafka-streams-app")
}

class MainKafkaStreamsApp extends BaseKafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology = {

    implicit val purchaseSerde = new CustomObjectSerde[Purchase]
    implicit val rewardSerde = new CustomObjectSerde[Reward]

    val sourceTopic:String = AppConstant.TOPIC_TRANSACTION
    val rewardsTopic:String = AppConstant.TOPIC_REWARDS

    val purchaseStream = builder.stream[String, Purchase](sourceTopic:String)
    purchaseStream.print(Printed.toSysOut[String, Purchase].withLabel(sourceTopic:String))

    val faker = new Faker
    purchaseStream
      .mapValues(purchase => Reward(purchase.customerId, faker.number.numberBetween(1000, 50000).toDouble, faker.number.numberBetween(1000, 5000)))
      .to(rewardsTopic)

    builder.build()
  }
}
