package it.kirsoft.examples.booksanalysisspark

import it.kirsoft.examples.booksanalysisspark.config.ConfigCreationException
import org.scalatest.funsuite.AnyFunSuite

class MainTest extends AnyFunSuite {

  test("Main.createConfig not enough params") {
    assertThrows[InputParamsException](Main.createConfig(Array("one_param")))
  }

  test("Main.createConfig load needs the third param, to reach the input dataset") {
    assertThrows[InputParamsException](Main.createConfig(Array("application.conf", "load")))
  }

  test("Main.createConfig matching only the correct job names") {
    assertThrows[InputParamsException](Main.createConfig(Array("application.conf", "etl")))
  }

  test("Main.createConfig avoid more than necessary params") {
    assertThrows[InputParamsException](Main.createConfig(Array("1", "2", "3", "4")))
  }

  test("Main.createConfig error creating config for malformed config") {
    assertThrows[ConfigCreationException](Main.createConfig(
      Array("application-confexample-malformed.conf", "retrieve")
    ))
  }

  test("Main.createConfig error creating config for missing file config") {
    assertThrows[ConfigCreationException](Main.createConfig(
      Array("application2.conf", "retrieve")
    ))
  }
}
