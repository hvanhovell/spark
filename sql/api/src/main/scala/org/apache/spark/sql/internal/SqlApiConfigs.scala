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
package org.apache.spark.sql.internal

import java.util.Locale

import org.apache.spark.internal.config.SimpleConfigRegistry
import org.apache.spark.sql.types.{AtomicType, TimestampNTZType, TimestampType}

object SqlApiConfigs extends SimpleConfigRegistry {
  val CASE_SENSITIVE = buildConf("spark.sql.caseSensitive")
    .internal()
    .doc("Whether the query analyzer should be case sensitive or not. " +
      "Default to case insensitive. It is highly discouraged to turn on case sensitive mode.")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(false)

  val LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED =
    buildConf("spark.sql.legacy.allowNegativeScaleOfDecimal")
      .internal()
      .doc("When set to true, negative scale of Decimal type is allowed. For example, " +
        "the type of number 1E10BD under legacy mode is DecimalType(2, -9), but is " +
        "Decimal(11, 0) in non legacy mode.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val MAX_TO_STRING_FIELDS = buildConf("spark.sql.debug.maxToStringFields")
    .doc("Maximum number of fields of sequence-like entries can be converted to strings " +
      "in debug output. Any elements beyond the limit will be dropped and replaced by a" +
      """ "... N more fields" placeholder.""")
    .version("3.0.0")
    .intConf
    .createWithDefault(25)

  val LEGACY_SETOPS_PRECEDENCE_ENABLED =
    buildConf("spark.sql.legacy.setopsPrecedence.enabled")
      .internal()
      .doc("When set to true and the order of evaluation is not specified by parentheses, the " +
        "set operations are performed from left to right as they appear in the query. When set " +
        "to false and order of evaluation is not specified by parentheses, INTERSECT operations " +
        "are performed before any UNION, EXCEPT and MINUS operations.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(false)

  val LEGACY_EXPONENT_LITERAL_AS_DECIMAL_ENABLED =
    buildConf("spark.sql.legacy.exponentLiteralAsDecimal.enabled")
      .internal()
      .doc("When set to true, a literal with an exponent (e.g. 1E-30) would be parsed " +
        "as Decimal rather than Double.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val ANSI_ENABLED = buildConf("spark.sql.ansi.enabled")
    .doc("When true, Spark SQL uses an ANSI compliant dialect instead of being Hive compliant. " +
      "For example, Spark will throw an exception at runtime instead of returning null results " +
      "when the inputs to a SQL operator/function are invalid." +
      "For full details of this dialect, you can find them in the section \"ANSI Compliance\" of " +
      "Spark's documentation. Some ANSI dialect features may be not from the ANSI SQL " +
      "standard directly, but their behaviors align with ANSI SQL's style")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(sys.env.get("SPARK_ANSI_SQL_MODE").contains("true"))

  val ENFORCE_RESERVED_KEYWORDS = buildConf("spark.sql.ansi.enforceReservedKeywords")
    .doc(s"When true and '${ANSI_ENABLED.key}' is true, the Spark SQL parser enforces the ANSI " +
      "reserved keywords and forbids SQL queries that use reserved keywords as alias names " +
      "and/or identifiers for table, view, function, etc.")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(false)

  val DOUBLE_QUOTED_IDENTIFIERS = buildConf("spark.sql.ansi.doubleQuotedIdentifiers")
    .doc(s"When true and '${ANSI_ENABLED.key}' is true, Spark SQL reads literals enclosed in " +
      "double quoted (\") as identifiers. When false they are read as string literals.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(false)

  object TimestampTypes extends Enumeration {
    val TIMESTAMP_NTZ, TIMESTAMP_LTZ = Value
  }

  val TIMESTAMP_TYPE =
    buildConf("spark.sql.timestampType")
      .doc("Configures the default timestamp type of Spark SQL, including SQL DDL, Cast clause " +
        s"and type literal. Setting the configuration as ${TimestampTypes.TIMESTAMP_NTZ} will " +
        "use TIMESTAMP WITHOUT TIME ZONE as the default type while putting it as " +
        s"${TimestampTypes.TIMESTAMP_LTZ} will use TIMESTAMP WITH LOCAL TIME ZONE. " +
        "Before the 3.4.0 release, Spark only supports the TIMESTAMP WITH " +
        "LOCAL TIME ZONE type.")
      .version("3.4.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(TimestampTypes.values.map(_.toString))
      .createWithDefault(TimestampTypes.TIMESTAMP_LTZ.toString)

  def caseSensitive: Boolean = getConf(CASE_SENSITIVE)

  def allowNegativeScaleOfDecimalEnabled: Boolean =
    getConf(LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED)

  def maxToStringFields: Int =
    getConf(MAX_TO_STRING_FIELDS)

  def setOpsPrecedenceEnforced: Boolean =
    getConf(LEGACY_SETOPS_PRECEDENCE_ENABLED)

  def exponentLiteralAsDecimalEnabled: Boolean =
    getConf(LEGACY_EXPONENT_LITERAL_AS_DECIMAL_ENABLED)

  def ansiEnabled: Boolean =
    getConf(ANSI_ENABLED)

  def enforceReservedKeywords: Boolean =
    ansiEnabled && getConf(ENFORCE_RESERVED_KEYWORDS)

  def doubleQuotedIdentifiers: Boolean =
    ansiEnabled && getConf(DOUBLE_QUOTED_IDENTIFIERS)

  def timestampType: AtomicType = getConf(TIMESTAMP_TYPE) match {
    case "TIMESTAMP_LTZ" =>
      // For historical reason, the TimestampType maps to TIMESTAMP WITH LOCAL TIME ZONE
      TimestampType

    case "TIMESTAMP_NTZ" =>
      TimestampNTZType
  }
}
