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
package org.apache.spark.sql.errors

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.internal.{SqlApiConfigs, StringUtils}
import org.apache.spark.sql.types.{DayTimeIntervalType, YearMonthIntervalType}

trait SqlApiErrors {
  def attributeNameSyntaxError(name: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1049",
      messageParameters = Map("name" -> name),
      cause = null)
  }

  def notUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2227",
      messageParameters = Map(
        "name" -> name,
        "userClass" -> userClass),
      cause = null)
  }

  def cannotLoadUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2228",
      messageParameters = Map(
        "name" -> name,
        "userClass" -> userClass),
      cause = null)
  }

  def invalidDayTimeField(field: Byte): Throwable = {
    val supportedIds = DayTimeIntervalType.dayTimeFields
      .map(i => s"$i (${DayTimeIntervalType.fieldToString(i)})")
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1223",
      messageParameters = Map(
        "field" -> field.toString,
        "supportedIds" -> supportedIds.mkString(", ")),
      cause = null)
  }

  def invalidDayTimeIntervalType(startFieldName: String, endFieldName: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1224",
      messageParameters = Map(
        "startFieldName" -> startFieldName,
        "endFieldName" -> endFieldName),
      cause = null)
  }

  def invalidYearMonthField(field: Byte): Throwable = {
    val supportedIds = YearMonthIntervalType.yearMonthFields
      .map(i => s"$i (${YearMonthIntervalType.fieldToString(i)})")
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1225",
      messageParameters = Map(
        "field" -> field.toString,
        "supportedIds" -> supportedIds.mkString(", ")),
      cause = null)
  }

  def unsupportedArrayTypeError(clazz: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2120",
      messageParameters = Map("clazz" -> clazz.toString()))
  }

  def unsupportedJavaTypeError(clazz: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2121",
      messageParameters = Map("clazz" -> clazz.toString()))
  }

  def decimalCannotGreaterThanPrecisionError(scale: Int, precision: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1228",
      messageParameters = Map(
        "scale" -> scale.toString,
        "precision" -> precision.toString),
      cause = null)
  }

  def decimalOnlySupportPrecisionUptoError(decimalType: String, precision: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1229",
      messageParameters = Map(
        "decimalType" -> decimalType,
        "precision" -> precision.toString),
      cause = null)
  }

  def negativeScaleNotAllowedError(scale: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1230",
      messageParameters = Map(
        "scale" -> scale.toString,
        "config" -> SqlApiConfigs.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key),
      cause = null)
  }

  def schemaFailToParseError(schema: String, e: Throwable): Throwable = {
    new SparkException(
      errorClass = "INVALID_SCHEMA.PARSE_ERROR",
      messageParameters = Map(
        "inputSchema" -> StringUtils.quoteByDefault(schema),
        "reason" -> e.getMessage
      ),
      cause = e)
  }
}

object SqlApiErrors extends SqlApiErrors
