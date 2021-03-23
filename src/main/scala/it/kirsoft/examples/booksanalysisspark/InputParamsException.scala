package it.kirsoft.examples.booksanalysisspark

final case class InputParamsException(private val message: String = "") extends Exception(message)
