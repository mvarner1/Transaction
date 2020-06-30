package com.varner.streams.demo.builder

import java.util.Date

import com.varner.streams.demo.model.Purchase
import org.apache.commons.lang3.StringUtils

object PurchaseBuilder {
  private val CREDIT_CARD_PREFIX = "xxxx-xxxx-xxxx-"

  def maskCreditCard(sourcePurchase: Purchase): Purchase = {
    val creditCardNumber: String = sourcePurchase.creditCardNumber
    if (StringUtils.isEmpty(creditCardNumber)) {
      throw new RuntimeException("creditCardNumber can't be empty")
    }
    var maskedCreditCardNumber: String = "xxxx"
    val parts = creditCardNumber.split("-")

    val last4Digits = creditCardNumber.split("-")(parts.length -1)
    maskedCreditCardNumber = CREDIT_CARD_PREFIX + last4Digits



    val purchaseWithMaskedCardNo: Purchase = Purchase(
      sourcePurchase.name, sourcePurchase.customerId,
      maskedCreditCardNumber, sourcePurchase.itemPurchased, sourcePurchase.quantity, sourcePurchase.price,
      sourcePurchase.purchaseDate, sourcePurchase.zipCode
    )

    purchaseWithMaskedCardNo


  }
}
