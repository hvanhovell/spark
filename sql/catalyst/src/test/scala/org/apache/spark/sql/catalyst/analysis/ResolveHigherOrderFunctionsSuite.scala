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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.expressions.{ArrayReduce, ArrayTransform, Expression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, IntegerType}

/**
 * Test suite for [[ResolveHigherOrderFunctions]].
 */
class ResolveHigherOrderFunctionsSuite extends PlanTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private val conf = SimpleCatalystConf(caseSensitiveAnalysis = false)

  object Analyzer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Resolution", FixedPoint(4), ResolveHigherOrderFunctions(conf)) :: Nil
  }

  private val key = 'key.int
  private val values1 = 'values.array(IntegerType)
  private val values2 = 'values.array(ArrayType(ArrayType(IntegerType)))
  private val data = LocalRelation(Seq(key, values1, values2))
  private val lvInt = NamedLambdaVariable("x", IntegerType, nullable = true)
  private val lvArray = NamedLambdaVariable("x", ArrayType(IntegerType), nullable = true)

  private def plan(e: Expression): LogicalPlan = data.select(e.as("res"))

  private def checkExpression(e1: Expression, e2: Expression): Unit = {
    comparePlans(Analyzer.execute(plan(e1)), plan(e2))
  }

  test("resolution - no op") {
    checkExpression(key, key)
  }

  test("resolution - simple") {
    val in = ArrayTransform(values1, LambdaFunction('x.attr + 1, Seq("x")))
    val out = ArrayTransform(values1, lvInt + 1, Seq(lvInt))
    checkExpression(in, out)
  }

  test("resolution - nested") {
    val in = ArrayTransform(values2, LambdaFunction(
      ArrayTransform('x.attr, LambdaFunction('x.attr + 1, Seq("x"))), Seq("x")))
    val out = ArrayTransform(values2,
      ArrayTransform(lvArray, lvInt + 1, Seq(lvInt)), Seq(lvArray))
    checkExpression(in, out)
  }

  test("fail - name collisions") {
    val p = plan(ArrayReduce(values1, key, LambdaFunction('x.attr + 'X.attr, Seq("x", "X"))))
    val msg = intercept[AnalysisException](Analyzer.execute(p)).getMessage
    assert(msg.contains("arguments should not have names that are semantically the same"))
  }

  test("fail - lambda arguments") {
    val p = plan(ArrayReduce(values1, key, LambdaFunction('x.attr, Seq("x"))))
    val msg = intercept[AnalysisException](Analyzer.execute(p)).getMessage
    assert(msg.contains("does not match the number of arguments expected"))
  }
}
