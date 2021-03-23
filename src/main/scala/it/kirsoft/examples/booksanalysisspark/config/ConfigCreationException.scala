package it.kirsoft.examples.booksanalysisspark.config

case class ConfigCreationException(private val message: String = "",
                                   private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
