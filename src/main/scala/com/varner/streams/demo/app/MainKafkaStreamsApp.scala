package com.varner.streams.demo.app

import com.varner.streams.demo.AppConstant
import com.varner.streams.demo.builder.{PurchaseBuilder, PurchasePatternBuilder, RewardBuilder}
import com.varner.streams.demo.model.{Purchase, PurchasePattern, Reward}
import com.varner.streams.demo.util.CustomObjectSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream

object MainKafkaStreamsApp extends App {
  new MainKafkaStreamsApp().start("main-kafka-streams-app")
}

class MainKafkaStreamsApp extends BaseKafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology = {

    implicit val purchaseSerde = new CustomObjectSerde[Purchase]
    implicit val rewardSerde = new CustomObjectSerde[Reward]
    implicit val purchasePatternSerde = new CustomObjectSerde[PurchasePattern]

    val sourceTopic:String = AppConstant.TOPIC_TRANSACTION
    val rewardsTopic:String = AppConstant.TOPIC_REWARDS
    val purchasePatternsTopic:String = AppConstant.TOPIC_PURCHASE_PATTERNS
    val purchaseStorageTopic:String = AppConstant.TOPIC_PURCHASE_STORAGE

    // Source Purchase Messages
    val purchaseStream: KStream[String, Purchase] = builder
        .stream[String, Purchase](sourceTopic:String)
        .mapValues(purchase => PurchaseBuilder.maskCreditCard(purchase))

    purchaseStream.print(Printed.toSysOut[String, Purchase].withLabel(sourceTopic:String))

    // Build PurchasePattern Messages from Source Purchase Messages and sink to purchasePatternsTopic
    val purchasePatternStream:KStream[String, PurchasePattern] = purchaseStream.mapValues(purchase => PurchasePatternBuilder.buildPurchasePattern(purchase))
    purchasePatternStream.print(Printed.toSysOut[String, PurchasePattern].withLabel(purchasePatternsTopic:String))
    purchasePatternStream.to(purchasePatternsTopic)

    // Build Reward Messages from Source Purchase Messages and sink rewardsTopic
    val rewardStream:KStream[String, Reward] = purchaseStream.mapValues(purchase => RewardBuilder.build(purchase))
    rewardStream.print(Printed.toSysOut[String, Reward].withLabel(rewardsTopic:String))
    rewardStream.to(rewardsTopic)

    // Sink Source Purchase(with masked card numbers) Messages to purchaseStorageTopic
    purchaseStream.to(purchaseStorageTopic)

    builder.build()
  }
}
