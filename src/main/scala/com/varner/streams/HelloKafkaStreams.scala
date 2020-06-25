package com.varner.streams

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder

object HelloKafkaStreams extends App {
  new HelloKafkaStreams().start("hello-kafka-streams")
}

class HelloKafkaStreams extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    val bs = builder.stream[String, String]("transaction")

    bs.mapValues(name => s"Pattern, $name!").to("Pattern")
    bs.mapValues(name => s"Rewards, $name!").to("Rewards")
    bs.mapValues(name => s"Storage, $name!").to("Storage")

    builder.build()
  }
}
