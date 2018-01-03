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
        replace(bd, BigDecimal(bd.toString.map(_ => 9).mkString), sign)
    }
  }

  /**
    * Get the max value for type.
    *
    * @param value The to find it's type.
    * @param sign  The sign that will be used to generate new value.
    * @return The new value.
    */
  def maxValue(value: AnyVal, sign: Boolean): BigDecimal = {
    if (sign) {
      BigDecimal(value match {
        case _: Byte => Byte.MaxValue
        case _: Short => Short.MaxValue
        case _: Int => Int.MaxValue
        case _: Long => Long.MaxValue
        case _: Float => Float.MaxValue
        case _: Double => Double.MaxValue
      })
    } else {
      BigDecimal(value match {
        case _: Byte => Byte.MinValue
        case _: Short => Short.MinValue
        case _: Int => Int.MinValue
        case _: Long => Long.MinValue
        case _: Float => Float.MinValue
        case _: Double => Double.MinValue
      }) * -1
    }
  }

  def absValue(value: Any): Any = {
    if (value.toString.charAt(0) == '-') {
      value match {
        case b: Byte => (-1 * b).toByte
        case s: Short => (-1 * s).toShort
        case i: Int => -1 * i
        case l: Long => -1 * l
        case f: Float => -1 * f
        case d: Double => -1 * d
      }
    } else {
      value
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
  def replace(value: Any, range: BigDecimal, sign: Boolean): String = {
    var cpt = 0
    s"${if (!sign) "-" else ""}${
      absValue(value).toString.map {
        case '.' =>
          cpt = cpt + 1
          '.'
        case _ =>
          val rngStr = range.bigDecimal.toPlainString
          val i = rngStr.charAt(cpt).toString.toInt + 1
          val v = Random.nextInt(i)
          cpt = cpt + 1
          v
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
