package com.varner.streams.demo.builder

import com.varner.streams.demo.model.{Purchase, PurchasePattern, Reward}

object RewardBuilder {
  def build(sourcePurchase: Purchase): Reward = {
    val totalPurchaseAmount:Double = sourcePurchase.price * (sourcePurchase.quantity * 1.0)
    val totalRewardPoints = totalPurchaseAmount.toInt
    val reward: Reward = Reward(
      sourcePurchase.customerId,
      totalPurchaseAmount,
      totalRewardPoints
    )
    reward
  }
}
