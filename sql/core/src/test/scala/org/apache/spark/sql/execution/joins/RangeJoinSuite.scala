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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.IntegerType

class RangeJoinSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.localSeqToDataFrameHolder

  val intervalKeys = Seq.tabulate(2)(BoundReference(_, IntegerType, nullable = false))
  private lazy val intervals1 = Seq(
    (-1, 0),
    (0, 1),
    (0, 2),
    (1, 5)
  ).toDF("low1", "high1")

  private lazy val intervals2 = Seq(
    (-2, -1),
    (-4, -2),
    (1, 3),
    (5, 7)
  ).toDF("low2", "high2")

  val pointKeys = Seq.fill(2)(BoundReference(0, IntegerType, nullable = false))
  private lazy val points = Seq(-3, 1, 3, 6).map(Tuple1.apply).toDF("point")

  test("interval-point range join") {
    // low1 <= point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        pointKeys,
        true :: false :: Nil,
        BuildRight,
        left,
        right),
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    // low1 <= point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        pointKeys,
        false :: false :: Nil,
        BuildRight,
        left,
        right),
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    // low <= point && point <= high1
    checkAnswer2(points, intervals1, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        pointKeys,
        intervalKeys,
        true :: true :: Nil,
        BuildRight,
        left,
        right),
      Seq(
        (1, 0, 1),
        (1, 0, 2),
        (1, 1, 5),
        (3, 1, 5)
      ).map(Row.fromTuple))

    // low1 < point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        pointKeys,
        false :: false :: Nil,
        BuildLeft,
        left,
        right),
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))
  }

  test("interval-interval range join") {
    // low1 <= high2 && low2 < high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        intervalKeys,
        true :: false :: Nil,
        BuildRight,
        left,
        right),
      Seq(
        (-1, 0, -2, -1),
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))

    // low1 < high2 && low2 <= high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        intervalKeys,
        false :: true :: Nil,
        BuildLeft,
        left,
        right),
      Seq(
        (0, 1, 1, 3),
        (0, 2, 1, 3),
        (1, 5, 1, 3),
        (1, 5, 5, 7)
      ).map(Row.fromTuple))

    // low1 < high2 && low2 < high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        intervalKeys,
        false :: false :: Nil,
        BuildRight,
        left,
        right),
      Seq(
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))

    // low1 <= high2 && low2 <= high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(
        intervalKeys,
        intervalKeys,
        true :: true :: Nil,
        BuildLeft,
        left,
        right),
      Seq(
        (-1, 0, -2, -1),
        (0, 1, 1, 3),
        (0, 2, 1, 3),
        (1, 5, 1, 3),
        (1, 5, 5, 7)
      ).map(Row.fromTuple))
  }
}
