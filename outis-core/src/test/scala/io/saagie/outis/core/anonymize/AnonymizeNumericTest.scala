package io.saagie.outis.core.anonymize

import org.scalatest.{Matchers, WordSpec}

class AnonymizeNumericTest extends WordSpec with Matchers {

  "substituteByte" when {
    "called" should {
      "return a byte" in {
        val result = AnonymizeNumeric.substituteByte(55.toByte, null)
        result.getClass shouldBe classOf[Byte]
        result should not be 55.toByte
      }
    }
  }

  "substituteFloat" when {
    "called" should {
      "return a float" in {
        val result = AnonymizeNumeric.substituteFloat(53.099998474121094.toFloat, null)
        result.getClass shouldBe classOf[Float]
        result should not be 53.099998474121094.toFloat
        println(result)
      }
    }
  }

  /*"substituteByte" when {
    "" should "return another byte" in {
      val result = AnonymizeNumeric.substituteByte(55.toByte, null)

      result.getClass shouldBe classOf[Byte]
      result should not be 55.toByte
    }
  }





  "substituteShort" should "return another short" in {
    val result = AnonymizeNumeric.substituteShort(55.toShort, null)

    result.getClass shouldBe classOf[Short]
    result should not be 55.toShort
  }

  "substituteInt" should "return another int" in {
    val result = AnonymizeNumeric.substituteInt(55, null)

    result.getClass shouldBe classOf[Int]
    result should not be 55
  }

  "substituteLong" should "return another long" in {
    val result = AnonymizeNumeric.substituteLong(55.toLong, null)

    result.getClass shouldBe classOf[Long]
    result should not be 55.toLong
  }

  "substituteFloat" should "return another float" in {
    val result = AnonymizeNumeric.substituteFloat(55.toFloat, null)

    result.getClass shouldBe classOf[Float]
    result should not be 55.toFloat
  }

  "substituteDouble" should "return another double" in {
    val result = AnonymizeNumeric.substituteDouble(55.toDouble, null)

    result.getClass shouldBe classOf[Double]
    result should not be 55.toDouble
  }

  "substituteBigDecimal" should "return another big decimal" in {
    val result = AnonymizeNumeric.substituteBigDecimal(BigDecimal(55), null)

    result.getClass shouldBe classOf[BigDecimal]
    result should not be BigDecimal(55)
  }*/
}
