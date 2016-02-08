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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * Reverse of explode.
 */
case class Implode(
    groupingExpressions: Seq[NamedExpression],
    implodedExpression: NamedExpression,
    child: SparkPlan) extends UnaryNode {

  private[this] val groupingAttributes: Seq[Attribute] = groupingExpressions.map(_.toAttribute)

  private[this] val implodedDataType: ArrayType = ArrayType(implodedExpression.dataType)

  private[this] val implodedAttribute: Attribute = AttributeReference(
    implodedExpression.name,
    implodedDataType,
    nullable = implodedExpression.nullable)()

  private[this] val implodedRowStruct: StructType =
    StructType.fromAttributes(Seq(implodedAttribute))

  override def output: Seq[Attribute] = groupingAttributes :+ implodedAttribute

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)
      val input = new GenericMutableRow(1)
      val converter = UnsafeProjection.create(implodedRowStruct)
      val result = GenerateUnsafeRowJoiner.create(
        StructType.fromAttributes(groupingAttributes),
        implodedRowStruct)

      grouped.map {
        case (group: UnsafeRow, iterator) =>
          input.update(0, new GenericArrayData(iterator.map(implodedExpression.eval).toArray))
          // Use MapObjects?
          result.join(group, converter(input))
      }
    }
  }
}
