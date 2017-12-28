package io.saagie.outis.core.anonymize

import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Random, Success, Try}

/**
  * Anonymization object for numerics
  */
object AnonymizeNumeric {

  def logger: Logger = Logger.getRootLogger

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
      case bd: BigDecimal =>
        replace(bd, bd.toString.map(_ => 9).mkString, sign)
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
    * @param sign  The sign of the generated value.
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
    *
    * @param b
    * @param errorAccumulator
    * @return
    */
  def substituteByte(b: Byte, errorAccumulator: LongAccumulator): Byte = {
    Try {
      substituteValue(b).toByte
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $b", e)
        errorAccumulator.add(1)
        b
    }
  }

  /**
    * Substitute a Short value.
    *
    * @param s
    * @param errorAccumulator
    * @return
    */
  def substituteShort(s: Short, errorAccumulator: LongAccumulator): Short = {
    Try {
      substituteValue(s).toShort
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $s", e)
        errorAccumulator.add(1)
        s
    }
  }

  /**
    * Substitute an Int value.
    *
    * @param i
    * @param errorAccumulator
    * @return
    */
  def substituteInt(i: Integer, errorAccumulator: LongAccumulator): Int = {
    Try {
      substituteValue(i).toInt
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $i", e)
        errorAccumulator.add(1)
        i
    }
  }

  /**
    * Substitute a Long value.
    *
    * @param l
    * @param errorAccumulator
    * @return
    */
  def substituteLong(l: Long, errorAccumulator: LongAccumulator): Long = {
    Try {
      substituteValue(l).toLong
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $l", e)
        errorAccumulator.add(1)
        l
    }
  }

  /**
    * Substitute a Float value.
    *
    * @param f
    * @param errorAccumulator
    * @return
    */
  def substituteFloat(f: Float, errorAccumulator: LongAccumulator): Float = {
    Try {
      substituteValue(f).toFloat
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $f", e)
        errorAccumulator.add(1)
        f
    }
  }

  /**
    * Substitute a Double value.
    *
    * @param d
    * @param errorAccumulator
    * @return
    */
  def substituteDouble(d: Double, errorAccumulator: LongAccumulator): Double = {
    Try {
      substituteValue(d).toDouble
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $d", e)
        errorAccumulator.add(1)
        d
    }
  }

  /**
    * Substitute a BigDecimal value.
    *
    * @param bd
    * @param errorAccumulator
    * @return
    */
  def substituteBigDecimal(bd: BigDecimal, errorAccumulator: LongAccumulator): BigDecimal = {
    Try {
      BigDecimal(substituteValue(bd))
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value ${bd.toString()}", e)
        errorAccumulator.add(1)
        bd
    }
  }
}
