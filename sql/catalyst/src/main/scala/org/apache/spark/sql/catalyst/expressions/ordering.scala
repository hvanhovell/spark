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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.{PhysicalArrayType, PhysicalAtomicType, PhysicalDataType, PhysicalStructType}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._


/**
 * A base class for generated/interpreted row ordering.
 */
class BaseOrdering extends Ordering[InternalRow] {
  def compare(a: InternalRow, b: InternalRow): Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * An interpreted row ordering comparator.
 */
class InterpretedOrdering(private[this] val ordering: IndexedSeq[SortOrder]) extends BaseOrdering {
  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(bindReferences(ordering, inputSchema).toIndexedSeq)

  def this(ordering: Seq[SortOrder]) = this(ordering.toIndexedSeq)

  private[this] val comparators = ordering.map { o =>
    PhysicalDataType(o.dataType) match {
      case dt: PhysicalAtomicType => dt.ordering.asInstanceOf[Ordering[Any]]
      case dt: PhysicalArrayType => dt.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case dt: PhysicalStructType => dt.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ => throw QueryExecutionErrors.orderedOperationUnsupportedByDataTypeError(o.dataType)
    }
  }



  override def compare(a: InternalRow, b: InternalRow): Int = {
    var i = 0
    val size = ordering.size
    while (i < size) {
      val order = ordering(i)
      val left = order.child.eval(a)
      val right = order.child.eval(b)

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.nullOrdering == NullsFirst) -1 else 1
      } else if (right == null) {
        return if (order.nullOrdering == NullsFirst) 1 else -1
      } else {
        val comparison = comparators(i).compare(left, right)
        if (comparison != 0) {
          return comparison
        }
      }
      i += 1
    }
    0
  }
}

object InterpretedOrdering {

  /**
   * Creates a [[InterpretedOrdering]] for the given schema, in natural ascending order.
   */
  def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

object RowOrdering extends CodeGeneratorWithInterpretedFallback[Seq[SortOrder], BaseOrdering] {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case dt: AtomicType => true
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
    case array: ArrayType => isOrderable(array.elementType)
    case udt: UserDefinedType[_] => isOrderable(udt.sqlType)
    case _ => false
  }

  /**
   * Returns true iff outputs from the expressions can be ordered.
   */
  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))

  override protected def createCodeGeneratedObject(in: Seq[SortOrder]): BaseOrdering = {
    GenerateOrdering.generate(in)
  }

  override protected def createInterpretedObject(in: Seq[SortOrder]): BaseOrdering = {
    new InterpretedOrdering(in)
  }

  def create(order: Seq[SortOrder], inputSchema: Seq[Attribute]): BaseOrdering = {
    createObject(bindReferences(order, inputSchema))
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  def createNaturalAscendingOrdering(dataTypes: Seq[DataType]): BaseOrdering = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    create(order, Seq.empty)
  }
}
