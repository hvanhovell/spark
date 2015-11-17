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
package org.apache.spark.sql.catalyst.expressions.codegen.bytecode

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.bytecode.GeneratorConstants._
import org.apache.spark.sql.catalyst.expressions.{Expression, SpecificMutableRow, GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{MapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.xbean.asm5.{Type, Opcodes, ClassVisitor}
import org.apache.xbean.asm5.commons.{Method, GeneratorAdapter}

import scala.reflect.ClassTag

// TODO add method for setNull! Decimal is special!
class MethodGenerator(
    m: Method,
    v: ClassVisitor,
    val inputRowType: Type = GeneratorConstants.internalRowType)
    extends GeneratorAdapter(Opcodes.ACC_PUBLIC, m, null, null, v) {
  import GeneratorConstants._

  /** Load the input row. */
  def loadInputRow(): Unit = loadArg(1)

  /** Check if a value is null. */
  def isNullAt(): Unit = invokeVirtual(inputRowType, GeneratorConstants.isNullAt)

  /** Get a value from an [[InternalRow]]. */
  def getValue(dataType: DataType): Unit = dataType match {
    case BooleanType => invokeVirtual(inputRowType, getBoolean)
    case ByteType => invokeVirtual(inputRowType, getByte)
    case ShortType => invokeVirtual(inputRowType, getShort)
    case IntegerType => invokeVirtual(inputRowType, getInt)
    case LongType => invokeVirtual(inputRowType, getLong)
    case FloatType => invokeVirtual(inputRowType, getFloat)
    case DoubleType => invokeVirtual(inputRowType, getDouble)
    case BinaryType => invokeVirtual(inputRowType, getBinary)
    case StringType => invokeVirtual(inputRowType, getUTF8String)
    case CalendarIntervalType => invokeVirtual(inputRowType, getInterval)
    case ArrayType(_) => invokeVirtual(inputRowType, getArray)
    case MapType(_) => invokeVirtual(inputRowType, getMap)
    case NullType => visitInsn(Opcodes.ACONST_NULL)
    case udt: UserDefinedType[_] => getValue(udt.sqlType)
    case dec: DecimalType =>
      push(dec.precision)
      push(dec.scale)
      invokeVirtual(inputRowType, getDecimal)
    case s: StructType =>
      visitLdcInsn(s.fields.length)
      invokeVirtual(inputRowType, getStruct)
    case _ =>
      visitInsn(Opcodes.ACONST_NULL)
      invokeVirtual(inputRowType, get)
  }

  /** Push the default value for a [[DataType]] to the stack. */
  def pushDefault(dataType: DataType): Unit = pushDefault(getType(dataType))

  /** Determine the default for a [[Type]]. */
  def determineDefault(dataType: Type): Int = dataType.getSort match {
    case Type.BOOLEAN => 0
    case Type.BYTE | Type.SHORT | Type.INT => 1
    case Type.LONG => 2
    case Type.FLOAT => 3
    case Type.DOUBLE => 4
    case _ => 5
  }

  /** Push the default value for [[Type]] to the stack. */
  def pushDefault(dataType: Type): Unit = determineDefault(dataType) match {
    case 0 => visitInsn(Opcodes.ICONST_0)
    case 1 => visitInsn(Opcodes.ICONST_M1)
    case 2 => visitLdcInsn(-1L)
    case 3 => visitLdcInsn(-1.0f)
    case 4 => visitLdcInsn(-1.0d)
    case 5 => visitInsn(Opcodes.ACONST_NULL)
  }

  /** Push a non-null boolean flag on the stack if we need it. */
  def pushNonNull(nullable: Boolean): Unit = {
    if (nullable) {
      push(false)
    }
  }
  def pop(dataType: Type): Unit = dataType.getSize match {
    case 2 => pop2()
    case 1 => pop()
  }

  /** Unbox a primitive wrapper for the given [[DataType]]. */
  def unbox(dataType: DataType): Unit = unbox(getType(dataType))

  /** */
  def nullSafeEval(in: DataType, out: DataType, nullable: Boolean)(generator: => Unit): Unit = {
    if (nullable) {
      val nullBranch = newLabel()
      val endIf = newLabel()
      visitJumpInsn(Opcodes.IFNE, nullBranch)
      generator
      goTo(endIf)
      mark(nullBranch)
      val i = getType(in)
      val o = getType(out)
      if (determineDefault(i) != determineDefault(o)) {
        pop(i)
        pushDefault(o)
      }
      push(true)
      mark(endIf)
    } else {
      generator
    }
  }
}

/**
  * Constants used during bytecode generation.
  */
object GeneratorConstants {
  /** Get the type of a class. */
  def getType[T](implicit tag: ClassTag[T]): Type = Type.getType(tag.runtimeClass)

  /** Type constants. */
  val generatedProjectionType = Type.getType("SpecificProjection")
  val expressionType = getType[Expression]
  val expressionArrayType = getType[Array[Expression]]
  val objectType = getType[Object]
  val stringType = getType[String]
  val optionType = getType[Option]
  val internalRowType = getType[InternalRow]
  val unsafeRowType = getType[UnsafeRow]
  val genericRowType = getType[GenericInternalRow]
  val specializedRowType = getType[SpecificMutableRow]
  val decimalType = getType[Decimal]
  val UTF8StringType = getType[UTF8String]
  val calendarIntervalType = getType[CalendarInterval]
  val arrayDataType = getType[ArrayData]
  val mapDataType = getType[MapData]
  val dataTypeType = getType[DataType]
  val binaryType = getType[Array[Byte]]

  /** Convert a Catalyst [[DataType]] into an ASM [[Type]]. */
  def getType(dataType: DataType): Type = dataType match {
    case BooleanType => Type.BOOLEAN_TYPE
    case ByteType => Type.BYTE_TYPE
    case ShortType => Type.SHORT_TYPE
    case IntegerType | DateType => Type.INT_TYPE
    case LongType | TimestampType => Type.LONG_TYPE
    case FloatType => Type.FLOAT_TYPE
    case DoubleType => Type.DOUBLE_TYPE
    case BinaryType => binaryType
    case StringType => UTF8StringType
    case CalendarIntervalType => calendarIntervalType
    case _: DecimalType => decimalType
    case _: StructType => internalRowType
    case _: ArrayType => arrayDataType
    case _: MapType => mapDataType
    case udt: UserDefinedType[_] => getType(udt.sqlType)
  }

  /** Create a method signature. */
  def method(name: String, ret: Type, args: Type*): Method =
    new Method(name, Type.getMethodDescriptor(ret, args: _*))

  /** Specialized Getter Methods. */
  val isNullAt = method("isNullAt", Type.BOOLEAN_TYPE, Type.INT_TYPE)
  val getBoolean = method("getBoolean", Type.BOOLEAN_TYPE, Type.INT_TYPE)
  val getByte = method("getByte", Type.BYTE_TYPE, Type.INT_TYPE)
  val getShort = method("getShort", Type.SHORT_TYPE, Type.INT_TYPE)
  val getInt = method("getInt", Type.INT_TYPE, Type.INT_TYPE)
  val getLong = method("getInt", Type.LONG_TYPE, Type.INT_TYPE)
  val getFloat = method("getFloat", Type.FLOAT_TYPE, Type.INT_TYPE)
  val getDouble = method("getDouble", Type.DOUBLE_TYPE, Type.INT_TYPE)
  val getDecimal = method("getDecimal", decimalType, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE)
  val getUTF8String = method("getUTF8String", UTF8StringType, Type.INT_TYPE)
  val getBinary = method("getBinary", binaryType, Type.INT_TYPE)
  val getInterval = method("getInternal", calendarIntervalType, Type.INT_TYPE)
  val getStruct = method("getStruct", internalRowType, Type.INT_TYPE, Type.INT_TYPE)
  val getArray = method("getArray", arrayDataType, Type.INT_TYPE)
  val getMap = method("getMap", mapDataType, Type.INT_TYPE)
  val get = method("getMap", objectType, Type.INT_TYPE, dataTypeType)

  /** Specialized Writer Methods - Shared. */
  val setNullAt = method("setNullAt", Type.VOID_TYPE, Type.INT_TYPE)

  /** Specialized Writer Methods - MutableRow. */
  val setBoolean = method("setBoolean", Type.VOID_TYPE, Type.INT_TYPE, Type.BOOLEAN_TYPE)
  val setByte = method("setByte", Type.VOID_TYPE, Type.INT_TYPE, Type.BYTE_TYPE)
  val setShort = method("setShort", Type.VOID_TYPE, Type.INT_TYPE, Type.SHORT_TYPE)
  val setInt = method("setInt", Type.VOID_TYPE, Type.INT_TYPE, Type.INT_TYPE)
  val setLong = method("setLong", Type.VOID_TYPE, Type.INT_TYPE, Type.LONG_TYPE)
  val setFloat = method("setFloat", Type.VOID_TYPE, Type.INT_TYPE, Type.FLOAT_TYPE)
  val setDouble = method("setDouble", Type.VOID_TYPE, Type.INT_TYPE, Type.DOUBLE_TYPE)
  val setDecimal = method("setDecimal", Type.VOID_TYPE, Type.INT_TYPE, decimalType, Type.INT_TYPE)
  val update = method("update", Type.VOID_TYPE, Type.INT_TYPE, objectType)

  /** Specialized Writer Methods - UnsafeRowWriter. */
  val writeBoolean = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.BOOLEAN_TYPE)
  val writeByte = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.BYTE_TYPE)
  val writeShort = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.SHORT_TYPE)
  val writeInt = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.INT_TYPE)
  val writeLong = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.LONG_TYPE)
  val writeFloat = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.FLOAT_TYPE)
  val writeDouble = method("write", Type.VOID_TYPE, Type.INT_TYPE, Type.DOUBLE_TYPE)
  val writeDecimal = method(
      "write", Type.VOID_TYPE, Type.INT_TYPE, decimalType, Type.INT_TYPE, Type.INT_TYPE)
  val writeInterval = method("write", Type.VOID_TYPE, Type.INT_TYPE, calendarIntervalType)
  val writeString = method("write", Type.VOID_TYPE, Type.INT_TYPE, UTF8StringType)
  val writeBinary = method("write", Type.VOID_TYPE, Type.INT_TYPE, binaryType)

  /** Expression 'eval' method. */
  val eval = method("eval", objectType, internalRowType)

  object Primitive {
    def unapply(dataType: DataType): Boolean = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        true
      case _ =>
        false
    }
  }
}