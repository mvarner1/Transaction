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
    builder.stream[String, String]("transaction")
      .mapValues(name => s"Pattern, $name!")
      .to("Pattern")

    builder.build()
  }
}
