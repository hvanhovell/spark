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

import java.util.Locale

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ParseTree

import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.SqlApiConfigs
import org.apache.spark.sql.parser.antlr.{SqlBaseParser, SqlBaseParserBaseVisitor}
import org.apache.spark.sql.parser.antlr.SqlBaseParser.{ColTypeContext, ColTypeListContext, CommentSpecContext, ComplexColTypeContext, ComplexColTypeListContext, ComplexDataTypeContext, DayTimeIntervalDataTypeContext, PrimitiveDataTypeContext, SingleTableSchemaContext, StringLitContext, YearMonthIntervalDataTypeContext}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, CharType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, MetadataBuilder, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VarcharType, YearMonthIntervalType}

/**
 * Visitor that converts text into a [[DataType]].
 */
class DataTypeBuilder extends SqlBaseParserBaseVisitor[AnyRef] {
  import ParserUtils._

  protected def typedVisit[T](ctx: ParseTree): T = ctx.accept(this).asInstanceOf[T]

  override def visitSingleDataType(ctx: SqlBaseParser.SingleDataTypeContext): DataType = {
    withOrigin(ctx)(typedVisit[DataType](ctx.dataType))
  }

  override def visitSingleTableSchema(ctx: SingleTableSchemaContext): StructType = {
    withOrigin(ctx)(StructType(visitColTypeList(ctx.colTypeList)))
  }

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float" | "real", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => SqlApiConfigs.timestampType
      case ("timestamp_ntz", Nil) => TimestampNTZType
      case ("timestamp_ltz", Nil) => TimestampType
      case ("string", Nil) => StringType
      case ("character" | "char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("decimal" | "dec" | "numeric", Nil) => DecimalType.USER_DEFAULT
      case ("decimal" | "dec" | "numeric", precision :: Nil) =>
        DecimalType(precision.getText.toInt, 0)
      case ("decimal" | "dec" | "numeric", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case ("void", Nil) => NullType
      case ("interval", Nil) => CalendarIntervalType
      case (dt @ ("character" | "char" | "varchar"), Nil) =>
        throw QueryParsingErrors.charTypeMissingLengthError(dt, ctx)
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw QueryParsingErrors.dataTypeUnsupportedError(dtStr, ctx)
    }
  }

  override def visitYearMonthIntervalDataType(ctx: YearMonthIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = YearMonthIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = YearMonthIntervalType.stringToField(endStr)
      if (end <= start) {
        throw QueryParsingErrors.fromToIntervalUnsupportedError(startStr, endStr, ctx)
      }
      YearMonthIntervalType(start, end)
    } else {
      YearMonthIntervalType(start)
    }
  }

  override def visitDayTimeIntervalDataType(ctx: DayTimeIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = DayTimeIntervalType.stringToField(startStr)
    if (ctx.to != null ) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = DayTimeIntervalType.stringToField(endStr)
      if (end <= start) {
        throw QueryParsingErrors.fromToIntervalUnsupportedError(startStr, endStr, ctx)
      }
      DayTimeIntervalType(start, end)
    } else {
      DayTimeIntervalType(start)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        StructType(Option(ctx.complexColTypeList).toArray.flatMap(visitComplexColTypeList))
    }
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(
      ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType)
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(
      name = identifier.getText,
      dataType = typedVisit(dataType()),
      nullable = NULL == null)
    Option(commentSpec).map(visitCommentSpec).map(structField.withComment).getOrElse(structField)
  }

  /**
   * Create a comment string.
   */
  override def visitCommentSpec(ctx: CommentSpecContext): String = withOrigin(ctx) {
    string(visitStringLit(ctx.stringLit))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType)
  }

  /**
   * Create a top level [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    Option(commentSpec()).map(visitCommentSpec).foreach {
      builder.putString("comment", _)
    }

    StructField(
      name = colName.getText,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = NULL == null,
      metadata = builder.build())
  }

  override def visitStringLit(ctx: StringLitContext): Token = {
    if (ctx != null) {
      if (ctx.STRING != null) {
        ctx.STRING.getSymbol
      } else {
        ctx.DOUBLEQUOTED_STRING.getSymbol
      }
    } else {
      null
    }
  }
}
