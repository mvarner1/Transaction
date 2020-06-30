package com.varner.streams.demo.builder

import com.varner.streams.demo.model.{Purchase, PurchasePattern}

object PurchasePatternBuilder {
  def buildPurchasePattern(sourcePurchase: Purchase): PurchasePattern = {
    val purchasePattern: PurchasePattern = PurchasePattern(
      sourcePurchase.customerId,
      sourcePurchase.itemPurchased, sourcePurchase.zipCode,
      sourcePurchase.purchaseDate, sourcePurchase.price
    )
    purchasePattern
  }
}
