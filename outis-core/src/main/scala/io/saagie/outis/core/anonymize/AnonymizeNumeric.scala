package io.saagie.outis.core.anonymize

import scala.util.Random

/**
  * Anonymization object for numerics
  */
object AnonymizeNumeric {

  /**
    * Replace values with appropiate range.
    *
    * @param value the value to replace.
    * @return The new value to String format.
    */
  def substituteValue(value: Any): String = {
    val sign = Random.nextBoolean()
    value match {
      case b: Byte =>
        val range = maxValue(b, sign)
        replace(value, range, sign).toString
      case s: Short =>
        val range = maxValue(s, sign)
        replace(value, range, sign).toString
      case i: Int => Random.nextInt()
        val range = maxValue(i, sign)
        replace(value, range, sign).toString
      case l: Long =>
        val range = maxValue(l, sign)
        replace(value, range, sign).toString
      case f: Float =>
        val range = maxValue(f, sign)
        replace(value, range, sign).toString
      case d: Double =>
        val range = maxValue(d, sign)
        replace(value, range, sign).toString
      case bd: BigDecimal => replace(bd, bd.toString.map(_ => 9).mkString, sign)
    }
  }

  /**
    * Get the max value for type.
    *
    * @param value The to find it's type.
    * @param sign  The sign that will be used to generate new value.
    * @return The new value.
    */
  def maxValue(value: AnyVal, sign: Boolean): AnyVal = {
    if (sign) {
      value match {
        case _: Byte => Byte.MaxValue
        case _: Short => Short.MaxValue
        case _: Int => Int.MaxValue
        case _: Long => Long.MaxValue
        case _: Float => Float.MaxValue
        case _: Double => Double.MaxValue
      }
    } else {
      (value match {
        case _: Byte => Byte.MaxValue
        case _: Short => Short.MaxValue
        case _: Int => Int.MaxValue
        case _: Long => Long.MaxValue
        case _: Float => Float.MaxValue
        case _: Double => Double.MaxValue
      }) * -1
    }
  }

  /**
    * Replace value's digits.
    *
    * @param value the value to replace.
    * @param range The range of the value.
    * @param sign The sign of the generated value.
    * @return String representation of the value.
    */
  def replace(value: Any, range: Any, sign: Boolean): String = {
    var cpt = 0
    s"${if (!sign) "-"}${
      Math.abs(value.toString.toLong).toString.map { _ =>
        cpt = cpt + 1
        Random.nextInt(range.toString.charAt(cpt).toInt + 1)
      }.mkString
    }"
  }

  /**
    * Substitute a Byte value.
    * @param b
    * @return
    */
  def substituteByte(b: Byte): Byte = {
    substituteValue(b).toByte
  }

  /**
    * Substitute a Short value.
    * @param s
    * @return
    */
  def substituteShort(s: Short): Short = {
    substituteValue(s).toShort
  }

  /**
    * Substitute an Int value.
    * @param i
    * @return
    */
  def substituteInt(i: Integer): Int = {
    substituteValue(i).toInt
  }

  /**
    * Substitute a Long value.
    * @param l
    * @return
    */
  def substituteLong(l: Long): Long = {
    substituteValue(l).toLong
  }

  /**
    * Substitute a Float value.
    * @param f
    * @return
    */
  def substituteFloat(f: Float): Float = {
    substituteValue(f).toFloat
  }

  /**
    * Substitute a Double value.
    * @param d
    * @return
    */
  def substituteDouble(d: Double): Double = {
    substituteValue(d).toDouble
  }

  /**
    * Substitute a BigDecimal value.
    * @param bd
    * @return
    */
  def substituteBigDecimal(bd: BigDecimal): BigDecimal = {
    BigDecimal(substituteValue(bd))
  }

}
