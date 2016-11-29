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

import java.util.TimeZone

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utility for getting the current timezone on the executor.
 */
object SessionTimeZoneID {
  val KEY = "spark.sql.timeZone"

  /**
   * Get the current session [[TimeZone]]. This will return the default [[TimeZone]] if no
   * session [[TimeZone]] has been set.
   */
  def get(): TimeZone = {
    val id = TaskContext.get().getLocalProperty(KEY)
    if (id != null) {
      TimeZone.getTimeZone(id)
    } else {
      TimeZone.getDefault
    }
  }
}

/**
 * Expression that returns the current session timezone ID.
 */
case class SessionTimeZoneID() extends LeafExpression with Nondeterministic {
  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  @transient private[this] var timeZone: UTF8String = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    timeZone = UTF8String.fromString(SessionTimeZoneID.get().getID())
  }

  override protected def evalInternal(input: InternalRow): UTF8String = {
    timeZone
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sessionTimeZoneName = classOf[SessionTimeZoneID].getCanonicalName
    val idTerm = ctx.freshName("timeZone")
    ctx.addMutableState("UTF8String", idTerm,
      s"$idTerm = UTF8String.fromString($sessionTimeZoneName.get().getID());")
    ev.copy(code = s"final ${ctx.javaType(dataType)} ${ev.value} = $idTerm;", isNull = "false")
  }
}
