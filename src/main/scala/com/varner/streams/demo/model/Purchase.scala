package com.varner.streams.demo.model

import java.util.Date

case class Purchase(
                      name: String,
                      customerId : String,
                      creditCardNumber : String,
                      itemPurchased : String,
                      quantity : Integer,
                      price : Double,
                      purchaseDate : Date,
                      zipCode : String
                    )
