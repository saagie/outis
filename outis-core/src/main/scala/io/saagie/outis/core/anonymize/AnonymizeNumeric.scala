package io.saagie.outis.core.anonymize

import java.nio.ByteBuffer

import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Random, Success, Try}

/**
  * Anonymization object for numerics
  */
object AnonymizeNumeric {

  def logger: Logger = Logger.getRootLogger

  def substituteValue[T](value: T): T = {
    val sign = Random.nextBoolean()
    value match {
      case b: Byte =>
        val range = maxValue(b, sign).toInt
        (b ^ Random.nextInt(range)).toByte.asInstanceOf[T]
      case s: Short =>
        val range = maxValue(s, sign).toInt
        (s ^ Random.nextInt(range)).toShort.asInstanceOf[T]
      case i: Int =>
        val range = maxValue(i, sign).toInt
        (i ^ Random.nextInt(range)).asInstanceOf[T]
      case l: Long =>
        (l ^ Random.nextLong()).asInstanceOf[T]
      case f: Float =>
//        val range = maxValue(1.toByte, sign).toInt
//        val bytes = ByteBuffer.allocate(4).putFloat(f).array().map(v => (v ^ Random.nextInt(range).toByte).toByte)
//        ByteBuffer.wrap(bytes).getFloat.asInstanceOf[T]
        Random.nextFloat().asInstanceOf[T]
      case d: Double =>
        val range = maxValue(1.toByte, sign).toInt
        val bytes = ByteBuffer.allocate(8).putDouble(d).array().map(v => (v ^ Random.nextInt(range).toByte).toByte)
        ByteBuffer.wrap(bytes).getDouble.asInstanceOf[T]
        Random.nextDouble().asInstanceOf[T]
      case bd: java.math.BigDecimal =>
        BigDecimal(Random.nextDouble()).bigDecimal.asInstanceOf[T]
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
    BigDecimal(value match {
      case _: Byte => Byte.MaxValue
      case _: Short => Short.MaxValue
      case _: Int => Int.MaxValue
      case _: Long => Long.MaxValue
      case _: Float => Float.MaxValue
      case _: Double => Double.MaxValue
    })
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
        case bd: BigDecimal => -1 * bd
      }
    } else {
      value
    }
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
      substituteValue(b)
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
      substituteValue(s)
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
      substituteValue(i)
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
      substituteValue(l)
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
      substituteValue(f)
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
      substituteValue(d)
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
  def substituteBigDecimal(bd: java.math.BigDecimal, errorAccumulator: LongAccumulator): java.math.BigDecimal = {
    //TODO: Totally wrecked
    Try {
      substituteValue(bd)
    } match {
      case Success(value) => value
      case Failure(e) =>
        logger.error(s"Impossible to process column value $bd", e)
        errorAccumulator.add(1)
        bd
    }
  }
}
