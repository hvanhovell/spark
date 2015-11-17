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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.xbean.asm5.commons.GeneratorAdapter
import org.apache.xbean.asm5.{Type, Opcodes}
import org.objectweb.asm.MethodAdapter

import scala.collection.mutable

class ExpressionCompiler(v: MethodGenerator, references: mutable.Buffer[Expression]) {
  compiler =>
  import GeneratorConstants._
  def apply(e: Expression): Unit = e match {
    case Literal(null, dataType) =>
      v.pushDefault(dataType)
      v.visitLdcInsn(true)
    case Literal(value, _: NumericType | BooleanType | DateType | TimestampType) =>
      v.visitLdcInsn(value)
    case m: LeafMathExpression =>
      v.visitLdcInsn(m.eval(null))
    case BoundReference(ordinal, dataType, nullable) =>

    case cast @ Cast(child, to) =>
      compile(child)
      val from = child.dataType
      val nullable = cast.nullable
      v.nullSafeEval(from, to, child.nullable) {
        (from, to) match {
          case (NullType, _) =>
            v.pushDefault(to)
            v.push(true)
          case x if x._1 == x._2 =>
            v.pushNonNull(nullable)
          case (Primitive(), Primitive()) =>
            v.cast(getType(from), getType(to))
            v.pushNonNull(nullable)
          case (BinaryType, StringType) =>
            v.invokeStatic(UTF8StringType, method("fromBytes", UTF8StringType, binaryType))
          case (TimestampType, StringType) =>
            v.invokeStatic()
          case (Primitive(), StringType) =>
            v.invokeStatic(stringType, method("valueOf", stringType, getType(to)))
        }
      }
    case other =>
      // Track the expression.
      val referenceId = references.size
      references += other

      // Emit the code.
      val dataType = getType(other.dataType)
      val expressionType = Type.getType(other.getClass)
      v.loadThis()
      v.getField(generatedProjectionType, "expressions", expressionArrayType)
      v.push(referenceId)
      v.arrayLoad(expressionType)
      v.visitInsn(Opcodes.AALOAD)
      v.loadInputRow()
      v.invokeVirtual(expressionType, eval)
      if (other.nullable) {
        v.visitInsn(Opcodes.DUP)
        val nullBranch = v.newLabel()
        val endIf = v.newLabel()
        v.ifNull(nullBranch)
        v.unbox(dataType)
        v.push(false)
        v.goTo(endIf)
        v.mark(nullBranch)
        v.pop()
        v.pushDefault(dataType)
        v.push(true)
        v.mark(endIf)
      } else {
        v.unbox(dataType)
      }
  }

  trait PartialEmitter {
    val function: PartialFunction[Expression, Unit]
  }

  /**
    * Bytecode emitter for Cast expressions.
    */
  object CastEmitter extends PartialEmitter {


    val function = {
      case cast @ Cast(child, to) =>
        compiler(child)
        val from = child.dataType
        val nullable = cast.nullable
        v.nullSafeEval(from, to, child.nullable) {
          to match {
            case _ if from == NullType =>
              v.pushDefault(to)
              v.push(false)
            case _ if from == to =>
              v.pushNonNull(nullable)
            case _ if isPrimitive(from) && isPrimitive(to) =>
              v.cast(getType(from), getType(to))
              v.pushNonNull(nullable)
            case StringType => castToString(from)
            case BinaryType => castToBinary(from)
              /*
            case DateType => castToDateCode(from, ctx)
            case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
            case TimestampType => castToTimestampCode(from, ctx)
            case CalendarIntervalType => castToIntervalCode(from)
            case BooleanType => castToBooleanCode(from)
            case ByteType => castToByteCode(from)
            case ShortType => castToShortCode(from)
            case IntegerType => castToIntCode(from)
            case FloatType => castToFloatCode(from)
            case LongType => castToLongCode(from)
            case DoubleType => castToDoubleCode(from)
            */
          }
        }
    }

    /** Check if the dataType matches a primitive. */
    private[this] def isPrimitive(dataType: DataType): Boolean = dataType match {
      case BooleanType | ByteType | ShortType |
           IntegerType | LongType | FloatType | DoubleType => true
      case _ => false
    }

    // String conversion methods descriptors.
    private[this] val fromString = method("fromString", UTF8StringType, stringType)
    private[this] val fromBytes = method("fromBytes", UTF8StringType, binaryType)
    private[this] val dateTimeUtilsType = Type.getType(DateTimeUtils.getClass)
    private[this] val dateToString = method("dateToString", UTF8StringType, Type.INT_TYPE)
    private[this] val tsToString = method("timestampToString", UTF8StringType, Type.LONG_TYPE)

    /** Emit string casting code. */
    private[this] def castToString(from: DataType): Unit = from match {
      case BinaryType =>
        v.invokeStatic(UTF8StringType, fromBytes)
      case DateType =>
        v.invokeStatic(dateTimeUtilsType, dateToString)
        v.invokeStatic(UTF8StringType, fromString)
      case TimestampType =>
        v.invokeStatic(dateTimeUtilsType, tsToString)
        v.invokeStatic(UTF8StringType, fromString)
      case ByteType | ShortType | IntegerType =>
        v.invokeStatic(stringType, method("valueOf", stringType, Type.INT_TYPE))
        v.invokeStatic(UTF8StringType, fromString)
      case BooleanType | LongType | FloatType | DoubleType =>
        v.invokeStatic(stringType, method("valueOf", stringType, getType(from)))
        v.invokeStatic(UTF8StringType, fromString)
      case _ =>
        v.invokeStatic(stringType, method("valueOf", stringType, objectType))
        v.invokeStatic(UTF8StringType, fromString)
    }

    /** Emit binary casting code. */
    private[this] def castToBinary(from: DataType): Unit = from match {
      case StringType =>
        v.invokeVirtual(UTF8StringType, method("getBytes", Type.VOID_TYPE, binaryType))
    }


    private[this] val stringToDate = method("stringToDate", optionType, UTF8StringType)
    private[this] val millisToDays = method("millisToDays", Type.INT_TYPE, Type.LONG_TYPE)
    private[this] val optIsDefined = method("isDefined", Type.BOOLEAN_TYPE, Type.VOID_TYPE)
    private[this] val optGet = method("get", objectType, Type.VOID_TYPE)

    private[this] def castToDate(from: DataType): Unit = from match {
      case StringType =>
        v.invokeStatic(dateTimeUtilsType, stringToDate)
        v.dup()
        v.invokeVirtual(optionType, optIsDefined)
        val nullBranch = v.newLabel()
        val endIf = v.newLabel()
        v.visitJumpInsn(Opcodes.IFNE, nullBranch)
        v.invokeVirtual(optionType, optGet)
        v.unbox(IntegerType)
        v.push(false)
        v.goTo(endIf)
        v.mark(nullBranch)
        v.pop()
        v.pushDefault(DateType)
        v.push(true)
        v.mark(endIf)
      case TimestampType =>
        v.push(1000L)
        v.math(GeneratorAdapter.DIV, Type.LONG_TYPE)
        v.invokeStatic(dateTimeUtilsType, millisToDays)
      case _ =>
        v.pushDefault(DateType)
        v.push(true)
    }
  }

  object BoundReferenceEmitter extends PartialEmitter {
    val function = {
      case BoundReference(ordinal, dataType, nullable) =>
        v.loadInputRow()
        if (nullable) {
          v.dup()
          v.push(ordinal)
          isNullAt()
          val nullBranch = v.newLabel()
          val endIf = v.newLabel()
          v.visitJumpInsn(Opcodes.IFNE, nullBranch)
          v.push(ordinal)
          getValue(dataType)
          v.push(false)
          v.goTo(endIf)
          v.mark(nullBranch)
          v.pop()
          v.pushDefault(dataType)
          v.push(true)
          v.mark(endIf)
        } else {
          getValue(dataType)
        }
    }

    /** Check if a value is null. */
    def isNullAt(): Unit = invoke("isNullAt", Type.BOOLEAN_TYPE)

    /** Get a value from an [[org.apache.spark.sql.catalyst.InternalRow]]. */
    def getValue(dataType: DataType): Unit = dataType match {
      case BooleanType => invoke("getBoolean", "(I)Z")
      case ByteType => invoke("getByte", "(I)B")
      case ShortType => invoke("getShort", "(I)S")
      case IntegerType => invoke("getInt", "(I)I")
      case LongType => invoke("getLong", "(I)J")
      case FloatType => invoke("getFloat", "(I)F")
      case DoubleType => invoke("getDouble", "(I)D")
      case BinaryType => invoke("getBinary", "(I)[B")
      case StringType => invoke("getUTF8String", UTF8StringType)
      case CalendarIntervalType => invoke("getInterval", calendarIntervalType)
      case ArrayType(_) => invoke("getArray", arrayDataType)
      case MapType(_) => invoke("getMap", mapDataType)
      case NullType => v.visitInsn(Opcodes.ACONST_NULL)
      case udt: UserDefinedType[_] => getValue(udt.sqlType)
      case dec: DecimalType =>
        v.push(dec.precision)
        v.push(dec.scale)
        invoke("getDecimal", decimalType, "(III)")
      case s: StructType =>
        v.visitLdcInsn(s.fields.length)
        invoke("getStruct", internalRowType, "(II)")
      case _ =>
        v.visitInsn(Opcodes.ACONST_NULL)
        invoke("get", internalRowType, s"(I${dataTypeType.getDescriptor})")
    }

    /** Invoke a getter method. */
    private[this] def invoke(name: String, retType: Type, argsTypes: String = "(I)"): Unit =
      invoke(name, argsTypes + retType.getDescriptor)

    /** Invoke a getter method. */
    private[this] def invoke(name: String, desc: String = "(I)"): Unit =
      v.visitMethodInsn(Opcodes.INVOKEVIRTUAL, v.inputRowType.getInternalName, name, desc, false)
  }

  /**
    * Bytecode emitter for expressions for which we cannot generate native bytecode. This will
    * fallback to calling the expressions eval(...) method.
    */
  object FallbackEmitter extends (Expression => Unit) {
    import GeneratorConstants._
    def apply(e: Expression): Unit = {
      // Track the expression.
      val referenceId = references.size
      references += e

      // Emit the code.
      val dataType = getType(e.dataType)
      val expressionType = Type.getType(e.getClass)
      v.loadThis()
      v.getField(generatedProjectionType, "expressions", expressionArrayType)
      v.push(referenceId)
      v.arrayLoad(expressionType)
      v.visitInsn(Opcodes.AALOAD)
      v.loadInputRow()
      v.invokeVirtual(expressionType, eval)
      if (e.nullable) {
        v.visitInsn(Opcodes.DUP)
        val nullBranch = v.newLabel()
        val endIf = v.newLabel()
        v.ifNull(nullBranch)
        v.unbox(dataType)
        v.push(false)
        v.goTo(endIf)
        v.mark(nullBranch)
        v.pop()
        v.pushDefault(dataType)
        v.push(true)
        v.mark(endIf)
      } else {
        v.unbox(dataType)
      }
    }
  }
}