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

/**
 * Resolve higher order functions. A higher order function can take a LambdaFunction as its
 * function and this rule makes sure that lambda functions are properly bound to the higher
 * order function and the lambda variables it exposes.
 *
 * This function resolves lambda variables by name, and allows nested higher order functions
 * to define the same name; in this case resolution will use the most nested variable.
 */
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
    case h: HigherOrderFunction if h.inputResolved && !h.function.resolved =>
      // Bind the lambda function if we have to.
      val newH = h.function match {
        case lambda @ LambdaFunction(expr, arguments) =>
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
          h.bind(arguments, expr)
        case _ => h
      }

      // Resolve nested higher order functions and lambda variable references.
      val lambdaMap = newH.variables.map(v => canonicalizer(v.name) -> v).toMap
      newH.mapChildren(resolve(_, parentLambdaMap ++ lambdaMap))

    case l: LambdaFunction =>
      // Do not resolve a lambda function. They can only be resolved in combination with a higher
      // order function.
      l

    case u @ UnresolvedAttribute(Seq(name)) =>
      parentLambdaMap.getOrElse(canonicalizer(name), u)

    case _ if !e.resolved =>
      e.mapChildren(resolve(_, parentLambdaMap))

    case _ =>
      e
  }
}
