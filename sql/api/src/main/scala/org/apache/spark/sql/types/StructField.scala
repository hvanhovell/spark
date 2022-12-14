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

package org.apache.spark.sql.types

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.internal.StringUtils
import org.apache.spark.util.StringConcat

/**
 * A field inside a StructType.
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 * @param metadata The metadata of this field. The metadata should be preserved during
 *                 transformation if the content of the column is not modified, e.g, in selection.
 *
 * @since 1.3.0
 */
@Stable
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty) {
  import StructField._

  /** No-arg constructor for kryo. */
  protected def this() = this(null, null)

  private[sql] def buildFormattedString(
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int): Unit = {
    if (maxDepth > 0) {
      stringConcat.append(s"$prefix-- ${StringUtils.escapeMetaCharacters(name)}: " +
        s"${dataType.typeName} (nullable = $nullable)\n")
      DataType.buildFormattedString(dataType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType,$nullable)"

  private[sql] def jsonValue: JValue = {
    ("name" -> name) ~
      ("type" -> dataType.jsonValue) ~
      ("nullable" -> nullable) ~
      ("metadata" -> metadata.jsonValue)
  }

  /**
   * Updates the StructField with a new comment value.
   */
  def withComment(comment: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString("comment", comment)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the comment of this StructField.
   */
  def getComment(): Option[String] = {
    if (metadata.contains("comment")) Option(metadata.getString("comment")) else None
  }

  /**
   * Updates the StructField with a new current default value.
   */
  def withCurrentDefaultValue(value: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, value)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Clears the StructField of its current default value, if any.
   */
  def clearCurrentDefaultValue(): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .remove(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the current default value of this StructField.
   */
  def getCurrentDefaultValue(): Option[String] = {
    if (metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      Option(metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Updates the StructField with a new existence default value.
   */
  def withExistenceDefaultValue(value: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, value)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the existence default value of this StructField.
   */
  private[sql] def getExistenceDefaultValue(): Option[String] = {
    if (metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY)) {
      Option(metadata.getString(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
    } else {
      None
    }
  }

  private def getDDLComment = getComment()
    .map(StringUtils.escapeSingleQuotedString)
    .map(" COMMENT '" + _ + "'")
    .getOrElse("")

  /**
   * Returns a string containing a schema in SQL format. For example the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId`: INT.
   */
  private[sql] def sql = s"${StringUtils.quoteIfNeeded(name)}: ${dataType.sql}$getDDLComment"

  /**
   * Returns a string containing a schema in DDL format. For example, the following value:
   * `StructField("eventId", IntegerType, false)` will be converted to `eventId` INT NOT NULL.
   * `StructField("eventId", IntegerType, true)` will be converted to `eventId` INT.
   * @since 2.4.0
   */
  def toDDL: String = {
    val nullString = if (nullable) "" else " NOT NULL"
    s"${StringUtils.quoteIfNeeded(name)} ${dataType.sql}${nullString}$getDDLComment"
  }
}

object StructField {
  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"

  // This column metadata represents the default value for all existing rows in a table after a
  // column has been added. This value is determined at time of CREATE TABLE, REPLACE TABLE, or
  // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default" to
  // be used by any scan when the columns in the source row are missing data. For example, consider
  // the following sequence:
  // CREATE TABLE t (c1 INT)
  // INSERT INTO t VALUES (42)
  // ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43)
  // SELECT c1, c2 FROM t
  // In this case, the final query is expected to return 42, 43. The ALTER TABLE ADD COLUMNS command
  // executed after there was already data in the table, so in order to enforce this invariant, we
  // need either (1) an expensive backfill of value 43 at column c2 into all previous rows, or (2)
  // indicate to each data source that selected columns missing data are to generate the
  // corresponding DEFAULT value instead. We choose option (2) for efficiency, and represent this
  // value as the text representation of a folded constant in the "EXISTS_DEFAULT" column metadata.
  val EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT"
}
