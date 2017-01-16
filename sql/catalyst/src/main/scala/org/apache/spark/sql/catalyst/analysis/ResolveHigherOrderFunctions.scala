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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ResolveHigherOrderFunctions(conf: CatalystConf) extends Rule[LogicalPlan] {
  type LambdaVariableMap = Map[String, NamedLambdaVariable]

  private val canonicalizer = {
    if (conf.caseSensitiveAnalysis) {
      s: String => s.toLowerCase
    } else {
      s: String => s
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case q: LogicalPlan =>
        q.mapExpressions(resolve(_, Map.empty))
    }
  }

  private def resolve(e: Expression, parentLambdaMap: LambdaVariableMap): Expression = e match {
    case h: HigherOrderFunction if h.inputResolved && h.function.isInstanceOf[LambdaFunction] =>
      val lambda @ LambdaFunction(expr, arguments) = h.function.asInstanceOf[LambdaFunction]

      // Make sure the lambda's arguments are not ambiguous.
      var i = 0
      val length = arguments.size
      while (i < length) {
        var j = i + 1
        while (j < length) {
          if (conf.resolver(arguments(i), arguments(j))) {
            lambda.failAnalysis(
              "Lambda function arguments should not have names that are semantically the " +
                s"same: '${arguments(i)}' @ $i and '${arguments(j)}' @ $j")
          }
          j += 1
        }
        i += 1
      }

      // Make sure the number of arguments given match the number of arguments required.
      if (arguments.size != h.numVariables) {
        lambda.failAnalysis(
          s"The number of lambda function arguments '${arguments.size}' does not " +
            "match the number of arguments expected by the higher order function " +
            s"'${h.numVariables}'.")
      }

      // Bind the the lambda variables to the higher order function
      val newH = h.bind(arguments, expr)

      // Resolve nested higher order functions and lambda variable references.
      newH.mapChildren(resolve(_, parentLambdaMap ++ createLambdaMap(newH.variables)))

    case h: HigherOrderFunction if h.resolved =>
      h.mapChildren(resolve(_, parentLambdaMap ++ createLambdaMap(h.variables)))

    case u @ UnresolvedAttribute(Seq(name)) =>
      parentLambdaMap.getOrElse(canonicalizer(name), u)

    case _ =>
      e.mapChildren(resolve(_, parentLambdaMap))
  }

  private def createLambdaMap(variables: Seq[NamedLambdaVariable]): LambdaVariableMap = {
    variables.map(v => canonicalizer(v.name) -> v).toMap
  }
}
