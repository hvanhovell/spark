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

package org.apache.spark.sql.execution.joins

import org.apache.spark.{broadcast, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.CompactBuffer

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class BroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin with CodegenSupport {

  val streamSideName = buildSide match {
    case BuildLeft => "right"
    case BuildRight => "left"
  }

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(
      canJoinKeyFitWithinLong,
      rewriteKeyExpr(buildKeys),
      buildPlan.output)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    streamedPlan.execute().mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashedRelation.getMemorySize)
      hashJoin(streamedIter, hashedRelation, numOutputRows)
    }
  }

  private var broadcastRelation: broadcast.Broadcast[HashedRelation] = _

  // the term for hash relation
  private var relationTerm: String = _

  override def upstream(): RDD[InternalRow] = {
    streamedPlan.asInstanceOf[CodegenSupport].upstream()
  }

  override def doProduce(ctx: CodegenContext): String = {
    // create a name for HashRelation
    broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    relationTerm = ctx.freshName("relation")
    val clsName = broadcastRelation.value.getClass.getName
    ctx.addMutableState(clsName, relationTerm,
      s"""
         | $relationTerm = ($clsName) $broadcast.value();
         | incPeakExecutionMemory($relationTerm.getMemorySize());
       """.stripMargin)

    s"""
       | ${streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)}
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // generate the key as UnsafeRow or Long
    ctx.currentVars = input
    val (keyVal, anyNull) = if (canJoinKeyFitWithinLong) {
      val expr = rewriteKeyExpr(streamedKeys).head
      val ev = BindReferences.bindReference(expr, streamedPlan.output).gen(ctx)
      (ev, ev.isNull)
    } else {
      val keyExpr = streamedKeys.map(BindReferences.bindReference(_, streamedPlan.output))
      val ev = GenerateUnsafeProjection.createCode(ctx, keyExpr)
      (ev, s"${ev.value}.anyNull()")
    }

    // find the matches from HashedRelation
    val matched = ctx.freshName("matched")

    // create variables for output
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    val buildColumns = buildPlan.output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).gen(ctx)
    }
    val resultVars = buildSide match {
      case BuildLeft => buildColumns ++ input
      case BuildRight => input ++ buildColumns
    }

    val numOutput = metricTerm(ctx, "numOutputRows")
    val outputCode = if (condition.isDefined) {
      // filter the output via condition
      ctx.currentVars = resultVars
      val ev = BindReferences.bindReference(condition.get, this.output).gen(ctx)
      s"""
         | ${ev.code}
         | if (!${ev.isNull} && ${ev.value}) {
         |   $numOutput.add(1);
         |   ${consume(ctx, resultVars)}
         | }
       """.stripMargin
    } else {
      s"""
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    }

    if (broadcastRelation.value.isInstanceOf[UniqueHashedRelation]) {
      s"""
         | // generate join key
         | ${keyVal.code}
         | // find matches from HashedRelation
         | UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyVal.value});
         | if ($matched != null) {
         |   ${buildColumns.map(_.code).mkString("\n")}
         |   $outputCode
         | }
     """.stripMargin

    } else {
      val matches = ctx.freshName("matches")
      val bufferType = classOf[CompactBuffer[UnsafeRow]].getName
      val i = ctx.freshName("i")
      val size = ctx.freshName("size")
      s"""
         | // generate join key
         | ${keyVal.code}
         | // find matches from HashRelation
         | $bufferType $matches = ${anyNull} ? null :
         |  ($bufferType) $relationTerm.get(${keyVal.value});
         | if ($matches != null) {
         |   int $size = $matches.size();
         |   for (int $i = 0; $i < $size; $i++) {
         |     UnsafeRow $matched = (UnsafeRow) $matches.apply($i);
         |     ${buildColumns.map(_.code).mkString("\n")}
         |     $outputCode
         |   }
         | }
     """.stripMargin
    }
  }
}
