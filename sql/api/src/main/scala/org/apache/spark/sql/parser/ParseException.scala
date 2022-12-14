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
package org.apache.spark.sql.parser

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.{QueryContext, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.internal.{CurrentOrigin, Origin}

/**
 * A [[ParseException]] is an [[Exception]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin,
    errorClass: Option[String] = None,
    messageParameters: Map[String, String] = Map.empty,
    queryContext: Array[QueryContext] = ParseException.getQueryContext())
  extends Exception(message)
  with SparkThrowable
  with Serializable {

  def this(errorClass: String, messageParameters: Map[String, String], ctx: ParserRuleContext) =
    this(Option(ParserUtils.command(ctx)),
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop),
      Some(errorClass),
      messageParameters)

  def this(errorClass: String, ctx: ParserRuleContext) = this(errorClass, Map.empty, ctx)

  /** Compose the message through SparkThrowableHelper given errorClass and messageParameters. */
  def this(
          command: Option[String],
          start: Origin,
          stop: Origin,
          errorClass: String,
          messageParameters: Map[String, String]) =
    this(
      command,
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      start,
      stop,
      Some(errorClass),
      messageParameters)

  override def getMessage: String = {
    val builder = new mutable.StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p), _, _, _, _, _) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    val (cls, params) =
      if (errorClass.contains("PARSE_SYNTAX_ERROR") && cmd.trim().isEmpty) {
        // PARSE_EMPTY_STATEMENT error class overrides the PARSE_SYNTAX_ERROR when cmd is empty
        (Some("PARSE_EMPTY_STATEMENT"), Map.empty[String, String])
      } else {
        (errorClass, messageParameters)
      }
    new ParseException(Option(cmd), message, start, stop, cls, params, queryContext)
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getErrorClass: String = errorClass.orNull

  override def getQueryContext: Array[QueryContext] = queryContext
}

object ParseException {
  def getQueryContext(): Array[QueryContext] = {
    val context = CurrentOrigin.get.context
    if (context.isValid) Array(context) else Array.empty
  }
}
