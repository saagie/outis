package io.saagie.outis.core.anonymize

import org.scalatest.{FlatSpec, Matchers}

class AnonymizeTest extends FlatSpec with Matchers {

  "suppression" should "return option none value" in {
    Anonymize.suppression() shouldBe None
  }

  "setTo with a char pattern to field" should "return an value with same size and only the char" in {
    Anonymize.setTo("MyFieltoAnonymise", '_') shouldBe "_________________"
  }

  "setToBlank on a field" should "return a blank value with same size" in {
    Anonymize.setToBlank("MyFieltoAnonymise") shouldBe "                 "
  }

  "setToX on a field" should "return a 'xxx...' value with the same size" in {
    Anonymize.setToX("MyFieltoAnonymise") shouldBe "XXXXXXXXXXXXXXXXX"
  }

  "truncate field" should "return a new value of x characters" in {
    Anonymize.truncate("MyFieldtoAnonymise", 7) shouldBe Right("MyField")
  }

  "truncate field" should "throw exception if value size is less than truncation size" in {
    Anonymize.truncate("MyFi", 7).left.get.message shouldBe
      "Can't truncate 'MyFi' because size is less or equal to truncation size of 7"
  }

  "substitute field" should "return a same format string with new value" in {
    val original = "john.doe1@saagie.com"
    val substitue = Anonymize.substitute(original)
    substitue should not be original
    substitue.length shouldEqual original.length
    substitue(4) shouldBe '.'
    substitue(8).isDigit shouldBe true
    substitue(9) shouldBe '@'
  }

}
