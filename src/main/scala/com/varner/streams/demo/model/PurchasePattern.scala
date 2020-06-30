package com.varner.streams.demo.model

import java.util.Date

case class PurchasePattern(customerId : String, item: String, zipCode: String, purchaseDate: Date, amount: Double)
