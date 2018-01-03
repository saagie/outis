package io.saagie.outis.core.anonymize

import org.scalatest.{FlatSpec, Matchers}

class AnonymizeNumericTest extends FlatSpec with Matchers {

  "SubstituteByte" should "return another byte" in {
    val result = AnonymizeNumeric.substituteByte(55.toByte, null)

    result.getClass shouldBe classOf[Byte]
    result should not be 55.toByte
  }

  "SubstituteShort" should "return another short" in {
    val result = AnonymizeNumeric.substituteShort(55.toShort, null)

    result.getClass shouldBe classOf[Short]
    result should not be 55.toShort
  }

  "SubstituteInt" should "return another int" in {
    val result = AnonymizeNumeric.substituteInt(55, null)

    result.getClass shouldBe classOf[Int]
    result should not be 55
  }

  "SubstituteLong" should "return another long" in {
    val result = AnonymizeNumeric.substituteLong(55.toLong, null)

    result.getClass shouldBe classOf[Long]
    result should not be 55.toLong
  }

  "SubstituteFloat" should "return another float" in {
    val result = AnonymizeNumeric.substituteFloat(55.toFloat, null)

    result.getClass shouldBe classOf[Float]
    result should not be 55.toFloat
  }

  "SubstituteDouble" should "return another double" in {
    val result = AnonymizeNumeric.substituteDouble(55.toDouble, null)

    result.getClass shouldBe classOf[Double]
    result should not be 55.toDouble
  }

  "SubstituteBigDecimal" should "return another big decimal" in {
    val result = AnonymizeNumeric.substituteBigDecimal(BigDecimal(55), null)

    result.getClass shouldBe classOf[BigDecimal]
    result should not be BigDecimal(55)
  }
}
