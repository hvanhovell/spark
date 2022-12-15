/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.types

import scala.reflect.runtime.universe.{typeTag, TypeTag}
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.InterpretedOrdering
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData, SQLOrderingUtil}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{ByteArray, CalendarInterval, UTF8String}

sealed abstract class PhysicalDataType {
  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int
}

object PhysicalDataType {
  def apply(dt: DataType): PhysicalDataType = dt match {
    case NullType => PhysicalNullType
    case BooleanType => PhysicalBooleanType
    case ByteType => PhysicalByteType
    case ShortType => PhysicalShortType
    case IntegerType => PhysicalIntegerType
    case LongType => PhysicalLongType
    case FloatType => PhysicalFloatType
    case DoubleType => PhysicalDoubleType
    case DecimalType.Fixed(p, s) => PhysicalDecimalType(p, s)
    case DateType => PhysicalIntegerType
    case TimestampType => PhysicalLongType
    case TimestampNTZType => PhysicalLongType
    case _: YearMonthIntervalType => PhysicalIntegerType
    case _: DayTimeIntervalType => PhysicalLongType
    case CalendarIntervalType => PhysicalCalendarIntervalType
    case BinaryType => PhysicalBinaryType
    case _: CharType => PhysicalStringType
    case _: VarcharType => PhysicalStringType
    case StringType => PhysicalStringType
    case st: StructType => PhysicalStructType(st)
    case at: ArrayType => PhysicalArrayType(at)
    case mt: MapType => PhysicalMapType(mt)
    case udt: UserDefinedType[_] => apply(udt.sqlType)
    case _ => UninitializedPhysicalType
  }
}

sealed abstract class TypedPhysicalDataType extends PhysicalDataType {
  private[sql] type InternalType
  private[sql] val cls: Class[InternalType]
}

object TypedPhysicalDataType {
  def apply(dt: DataType): TypedPhysicalDataType =
    PhysicalDataType(dt).asInstanceOf[TypedPhysicalDataType]
}

sealed abstract class OrderedPhysicalDataType extends TypedPhysicalDataType {
  private[sql] def ordering: Ordering[InternalType]
}

object OrderedPhysicalDataType {
  def apply(dt: DataType): OrderedPhysicalDataType =
    PhysicalDataType(dt).asInstanceOf[OrderedPhysicalDataType]

  def ordering(dt: DataType): Ordering[Any] = {
    try apply(dt).ordering.asInstanceOf[Ordering[Any]] catch {
      case NonFatal(_) =>
        throw QueryExecutionErrors.unsupportedTypeError(dt)
    }
  }
}

sealed abstract class PhysicalAtomicType extends OrderedPhysicalDataType {
  private[sql] val tag: TypeTag[InternalType]
}

sealed abstract class PhysicalNumericType extends PhysicalAtomicType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer a no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[InternalType]

  private[sql] def exactNumeric: Numeric[InternalType] = numeric
}

object PhysicalNumericType {
  def apply(nt: NumericType): PhysicalNumericType = {
    PhysicalDataType(nt).asInstanceOf[PhysicalNumericType]
  }
}

sealed abstract class PhysicalIntegralType extends PhysicalNumericType {
  private[sql] val integral: Integral[InternalType]
}

object PhysicalIntegralType {
  def apply(it: IntegralType): PhysicalIntegralType = {
    PhysicalDataType(it).asInstanceOf[PhysicalIntegralType]
  }
}

sealed abstract class PhysicalFractionalType extends PhysicalNumericType {
  private[sql] val fractional: Fractional[InternalType]
  private[sql] val asIntegral: Integral[InternalType]
}

object PhysicalFractionalType {
  def apply(ft: FractionalType): PhysicalFractionalType = {
    PhysicalDataType(ft).asInstanceOf[PhysicalFractionalType]
  }
}

class PhysicalBooleanType() extends PhysicalAtomicType {
  override private[sql] type InternalType = Boolean
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override def defaultSize: Int = 1
}

case object PhysicalBooleanType extends PhysicalBooleanType

class PhysicalByteType() extends PhysicalIntegralType {
  override private[sql] type InternalType = Byte
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Byte]]
  override private[sql] val integral = implicitly[Integral[Byte]]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override private[sql] val exactNumeric = ByteExactNumeric
  override def defaultSize: Int = 1
}

case object PhysicalByteType extends PhysicalByteType

class PhysicalShortType() extends PhysicalIntegralType {
  override private[sql] type InternalType = Short
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Short]]
  override private[sql] val integral = implicitly[Integral[Short]]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override private[sql] val exactNumeric = ShortExactNumeric
  override def defaultSize: Int = 2
}
case object PhysicalShortType extends PhysicalShortType

class PhysicalIntegerType() extends PhysicalIntegralType {
  override private[sql] type InternalType = Int
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Int]]
  override private[sql] val integral = implicitly[Integral[Int]]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override private[sql] val exactNumeric = IntegerExactNumeric
  override def defaultSize: Int = 4
}
case object PhysicalIntegerType extends PhysicalIntegerType

class PhysicalLongType() extends PhysicalIntegralType {
  override private[sql] type InternalType = Long
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Long]]
  override private[sql] val integral = implicitly[Integral[Long]]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override private[sql] val exactNumeric = LongExactNumeric
  override def defaultSize: Int = 8
}
case object PhysicalLongType extends PhysicalLongType

case class PhysicalDecimalType(precision: Int, scale: Int) extends PhysicalFractionalType {
  override private[sql] type InternalType = Decimal
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = Decimal.DecimalIsFractional
  override private[sql] val fractional = Decimal.DecimalIsFractional
  override private[sql] val ordering = Decimal.DecimalIsFractional
  override private[sql] val asIntegral = Decimal.DecimalAsIfIntegral
  override private[sql] def exactNumeric = DecimalExactNumeric
  override def defaultSize: Int = if (precision <= Decimal.MAX_LONG_DIGITS) 8 else 16
}

class PhysicalDoubleType() extends PhysicalFractionalType {
  override private[sql] type InternalType = Double
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Double]]
  override private[sql] val fractional = implicitly[Fractional[Double]]
  override private[sql] val ordering =
    (x: Double, y: Double) => SQLOrderingUtil.compareDoubles(x, y)
  override private[sql] val asIntegral = DoubleType.DoubleAsIfIntegral
  override private[sql] def exactNumeric = DoubleExactNumeric
  override def defaultSize: Int = 8
}
case object PhysicalDoubleType extends PhysicalDoubleType

class PhysicalFloatType() extends PhysicalFractionalType {
  override private[sql] type InternalType = Float
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val numeric = implicitly[Numeric[Float]]
  override private[sql] val fractional = implicitly[Fractional[Float]]
  override private[sql] val ordering = (x: Float, y: Float) => SQLOrderingUtil.compareFloats(x, y)
  override private[sql] val asIntegral = FloatType.FloatAsIfIntegral
  override private[sql] def exactNumeric = FloatExactNumeric
  override def defaultSize: Int = 4
}
case object PhysicalFloatType extends PhysicalFloatType

class PhysicalCalendarIntervalType() extends TypedPhysicalDataType {
  override private[sql] type InternalType = CalendarInterval
  override private[sql] val cls = classOf[InternalType]
  override def defaultSize: Int = 16
}
case object PhysicalCalendarIntervalType extends PhysicalCalendarIntervalType

class PhysicalBinaryType() extends PhysicalAtomicType {
  override private[sql] type InternalType = Array[Byte]
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val ordering =
    (x: Array[Byte], y: Array[Byte]) => ByteArray.compareBinary(x, y)
  override def defaultSize: Int = 100
}

case object PhysicalBinaryType extends PhysicalBinaryType

class PhysicalStringType() extends PhysicalAtomicType {
  override private[sql] type InternalType = UTF8String
  @transient override private[sql] lazy val tag = typeTag[InternalType]
  override private[sql] val cls = classOf[InternalType]
  override private[sql] val ordering = implicitly[Ordering[InternalType]]
  override def defaultSize: Int = 20
}

case object PhysicalStringType extends PhysicalStringType

case class PhysicalArrayType(
    elementType: DataType,
    containsNull: Boolean)
  extends OrderedPhysicalDataType {
  override private[sql] type InternalType = ArrayData
  override private[sql] val cls = classOf[InternalType]

  /**
   * The default size of a value of the ArrayType is the default size of the element type.
   * We assume that there is only 1 element on average in an array. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * elementType.defaultSize

  lazy val physicalElementType: PhysicalDataType = PhysicalDataType(elementType)

  @transient
  private[sql] lazy val ordering: Ordering[ArrayData] = new Ordering[ArrayData] {
    private[this] val elementOrdering: Ordering[Any] = physicalElementType match {
      case o: OrderedPhysicalDataType => o.ordering.asInstanceOf[Ordering[Any]]
      case other =>
        throw new IllegalArgumentException(s"Type $other does not support ordered operations")
    }

    def compare(x: ArrayData, y: ArrayData): Int = {
      val leftArray = x
      val rightArray = y
      val minLength = scala.math.min(leftArray.numElements(), rightArray.numElements())
      var i = 0
      while (i < minLength) {
        val isNullLeft = leftArray.isNullAt(i)
        val isNullRight = rightArray.isNullAt(i)
        if (isNullLeft && isNullRight) {
          // Do nothing.
        } else if (isNullLeft) {
          return -1
        } else if (isNullRight) {
          return 1
        } else {
          val comp =
            elementOrdering.compare(
              leftArray.get(i, elementType),
              rightArray.get(i, elementType))
          if (comp != 0) {
            return comp
          }
        }
        i += 1
      }
      if (leftArray.numElements() < rightArray.numElements()) {
        -1
      } else if (leftArray.numElements() > rightArray.numElements()) {
        1
      } else {
        0
      }
    }
  }
}

object PhysicalArrayType {
  def apply(at: ArrayType): PhysicalArrayType = PhysicalArrayType(at.elementType, at.containsNull)
}

case class PhysicalMapType(
    keyType: DataType,
    valueType: DataType,
    valueContainsNull: Boolean)
  extends TypedPhysicalDataType {
  override private[sql] type InternalType = MapData
  override private[sql] val cls = classOf[InternalType]
  /**
   * The default size of a value of the MapType is
   * (the default size of the key type + the default size of the value type).
   * We assume that there is only 1 element on average in a map. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * (keyType.defaultSize + valueType.defaultSize)
}

object PhysicalMapType {
  def apply(mt: MapType): PhysicalMapType = {
    PhysicalMapType(mt.keyType, mt.valueType, mt.valueContainsNull)
  }
}

case class PhysicalStructType(fields: Array[StructField]) extends OrderedPhysicalDataType {
  override private[sql] type InternalType = InternalRow
  override private[sql] val cls = classOf[InternalType]
  /**
   * The default size of a value of the StructType is the total default sizes of all field types.
   */
  override def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  @transient
  override private[sql] lazy val ordering =
    InterpretedOrdering.forSchema(fields.map(_.dataType))
}

object PhysicalStructType {
  def apply(st: StructType): PhysicalStructType = PhysicalStructType(st.fields)
}

class PhysicalNullType() extends PhysicalDataType {
  override def defaultSize: Int = 1
}
case object PhysicalNullType extends PhysicalNullType

object UninitializedPhysicalType extends PhysicalDataType {
  override def defaultSize: Int = 1
}
