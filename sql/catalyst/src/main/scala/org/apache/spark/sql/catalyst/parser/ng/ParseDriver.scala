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
package org.apache.spark.sql.catalyst.parser.ng

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser.{StringLiteralContext, DigitIdentifierContext, QuotedIdentifierContext}
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf}

object ParseDriver extends Logging {

  /** Create an LogicalPlan ASTNode from a SQL command. */
  def parsePlan(command: String, conf: ParserConf): ASTNode = parse(command, conf) { parser =>
    parser.statement()
  }

  /** Create an Expression ASTNode from a SQL command. */
  def parseExpression(command: String, conf: ParserConf): ASTNode = parse(command, conf) { parser =>
    parser.singleExpression()
  }

  // TODO add table identifier parsing.

  def parse[T](
      command: String,
      conf: ParserConf)(
      toTree: SqlBaseParser => ParserRuleContext): T = {
    // TODO conf is unused.


    logInfo(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)

    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    val tree = try {
      // first, try parsing with potentially faster SLL mode
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      toTree(parser)
    }
    catch {
      case e: ParseCancellationException =>
        // if we fail, parse with LL mode
        tokenStream.reset() // rewind input stream
        parser.reset()

        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        toTree(parser)
    }

    new AstBuilder().visit(tree).asInstanceOf[T]
  }
}

/**
 * This string stream provides the lexer with upper case characters only. This greatly simplifies
 * lexing the stream, while we can maintain the original command.
 *
 * This is based on Hive's org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
 *
 * The comment below (taken from the original class) describes the rationale for doing this:
 *
 * This class provides and implementation for a case insensitive token checker for the lexical
 * analysis part of antlr. By converting the token stream into upper case at the time when lexical
 * rules are checked, this class ensures that the lexical rules need to just match the token with
 * upper case letters as opposed to combination of upper case and lower case characters. This is
 * purely used for matching lexical rules. The actual token text is stored in the same way as the
 * user input without actually converting it into an upper case. The token values are generated by
 * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
 * function and is purely used for matching lexical rules. This also means that the grammar will
 * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
 * have the ANTLRNoCaseStringStream implementation.
 */

private[parser] class ANTLRNoCaseStringStream(input: String) extends ANTLRInputStream(input) {
  override def LA(i: Int): Int = {
    val la = super.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

case object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    throw new AnalysisException(msg, Some(line), Some(charPositionInLine))
  }
}

case object PostProcessor extends SqlBaseBaseListener {
  override def exitStringLiteral(ctx: StringLiteralContext): Unit = {

  }

  override def exitDigitIdentifier(ctx: DigitIdentifierContext): Unit = super.exitDigitIdentifier(ctx)

  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = super.exitQuotedIdentifier(ctx)
}
