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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.Origin
import org.apache.spark.sql.parser.AbstractSqlParser
import org.apache.spark.sql.parser.ParserUtils.withOrigin

/**
 * Concrete SQL parser for Catalyst-only SQL statements.
 */
class CatalystSqlParser extends AbstractSqlParser[AstBuilder] with ParserInterface {

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    val ctx = parser.singleExpression()
    withOrigin(ctx, Some(sqlText)) {
      builder.visitSingleExpression(ctx)
    }
  }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    builder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  /** Creates FunctionIdentifier for a given SQL string. */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      builder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
    }
  }

  /** Creates a multi-part identifier for a given SQL string */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    parse(sqlText) { parser =>
      builder.visitSingleMultipartIdentifier(parser.singleMultipartIdentifier())
    }
  }

  /** Creates LogicalPlan for a given SQL string of query. */
  override def parseQuery(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.query()
    withOrigin(ctx, Some(sqlText)) {
      builder.visitQuery(ctx)
    }
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.singleStatement()
    withOrigin(ctx, Some(sqlText)) {
      builder.visitSingleStatement(ctx) match {
        case plan: LogicalPlan => plan
        case _ =>
          val position = Origin(None, None)
          throw QueryParsingErrors.sqlStatementUnsupportedError(sqlText, position)
      }
    }
  }

  override protected def builder: AstBuilder = new AstBuilder
}

/** For test-only. */
object CatalystSqlParser extends CatalystSqlParser
