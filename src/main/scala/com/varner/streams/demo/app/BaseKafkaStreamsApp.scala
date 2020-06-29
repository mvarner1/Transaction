package com.varner.streams.demo.app

import java.time.Duration
import java.util.Properties

//import kafkastreams.demo.Settings.bootStrapServers
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

trait BaseKafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology

  def start(applicationId: String): KafkaStreams = {


    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    val builder = new StreamsBuilder
    val topology = createTopology(builder)

    val streams = new KafkaStreams(topology, config)
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() =>
      //streams.close(10, TimeUnit.SECONDS)
      streams.close(Duration.ofSeconds(10))

    ))

    streams
  }
}
