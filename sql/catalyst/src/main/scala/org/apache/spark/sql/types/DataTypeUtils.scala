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

import scala.collection.{Map, mutable}
import scala.util.Try
import scala.util.control.NonFatal
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy._

object DataTypeUtils {
  /**
   * Returns the normalized path to a field and the field in this struct and its child structs.
   *
   * If includeCollections is true, this will return fields that are nested in maps and arrays.
   */
  private[sql] def findNestedField(
      struct: StructType,
      fieldNames: Seq[String],
      includeCollections: Boolean = false,
      resolver: Resolver = _ == _,
      context: Origin = Origin()): Option[(Seq[String], StructField)] = {

    @scala.annotation.tailrec
    def findField(
                   struct: StructType,
                   searchPath: Seq[String],
                   normalizedPath: Seq[String]): Option[(Seq[String], StructField)] = {
      assert(searchPath.nonEmpty)
      val searchName = searchPath.head
      val found = struct.fields.filter(f => resolver(searchName, f.name))
      if (found.length > 1) {
        throw QueryCompilationErrors.ambiguousColumnOrFieldError(fieldNames, found.length, context)
      } else if (found.isEmpty) {
        None
      } else {
        val field = found.head
        val currentPath = normalizedPath :+ field.name
        val newSearchPath = searchPath.tail
        if (newSearchPath.isEmpty) {
          Some(normalizedPath -> field)
        } else {
          (newSearchPath, field.dataType) match {
            case (_, s: StructType) =>
              findField(s, newSearchPath, currentPath)

            case _ if !includeCollections =>
              throw QueryCompilationErrors.invalidFieldName(fieldNames, currentPath, context)

            case (Seq("key", rest @ _*), MapType(keyType, _, _)) =>
              findFieldInCollection(keyType, false, rest, currentPath, "key")

            case (Seq("value", rest @ _*), MapType(_, valueType, isNullable)) =>
              findFieldInCollection(valueType, isNullable, rest, currentPath, "value")

            case (Seq("element", rest @ _*), ArrayType(elementType, isNullable)) =>
              findFieldInCollection(elementType, isNullable, rest, currentPath, "element")

            case _ =>
              throw QueryCompilationErrors.invalidFieldName(fieldNames, currentPath, context)
          }
        }
      }
    }

    def findFieldInCollection(
        dt: DataType,
        nullable: Boolean,
        searchPath: Seq[String],
        normalizedPath: Seq[String],
        collectionFieldName: String): Option[(Seq[String], StructField)] = {
      if (searchPath.isEmpty) {
        Some(normalizedPath -> StructField(collectionFieldName, dt, nullable))
      } else {
        val newPath = normalizedPath :+ collectionFieldName
        dt match {
          case s: StructType =>
            findField(s, searchPath, newPath)
          case _ =>
            throw QueryCompilationErrors.invalidFieldName(fieldNames, newPath, context)
        }
      }
    }

    findField(struct, fieldNames, Nil)
  }

  private[sql] def fromString(raw: String): StructType = {
    Try(DataType.fromJson(raw)).getOrElse(LegacyTypeStringParser.parseString(raw)) match {
      case t: StructType => t
      case _ => throw QueryExecutionErrors.failedParsingStructTypeError(raw)
    }
  }

  private[sql] def toAttributes(struct: StructType): Seq[AttributeReference] =
    struct.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  private[sql] def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private[sql] def removeMetadata(key: String, dt: DataType): DataType =
    dt match {
      case StructType(fields) =>
        val newFields = fields.map { f =>
          val mb = new MetadataBuilder()
          f.copy(dataType = removeMetadata(key, f.dataType),
            metadata = mb.withMetadata(f.metadata).remove(key).build())
        }
        StructType(newFields)
      case _ => dt
    }

  /**
   * This leverages `merge` to merge data types for UNION operator by specializing
   * the handling of struct types to follow UNION semantics.
   */
  private[sql] def unionLikeMerge(left: DataType, right: DataType): DataType =
    mergeInternal(left, right, (s1: StructType, s2: StructType) => {
      val leftFields = s1.fields
      val rightFields = s2.fields
      require(leftFields.size == rightFields.size, "To merge nullability, " +
        "two structs must have same number of fields.")

      val newFields = leftFields.zip(rightFields).map {
        case (leftField, rightField) =>
          leftField.copy(
            dataType = unionLikeMerge(leftField.dataType, rightField.dataType),
            nullable = leftField.nullable || rightField.nullable)
      }
      StructType(newFields)
    })

  /**
   * Merges with another schema (`StructType`).  For a struct field A from `this` and a struct field
   * B from `that`,
   *
   * 1. If A and B have the same name and data type, they are merged to a field C with the same name
   *    and data type.  C is nullable if and only if either A or B is nullable.
   * 2. If A doesn't exist in `that`, it's included in the result schema.
   * 3. If B doesn't exist in `this`, it's also included in the result schema.
   * 4. Otherwise, `this` and `that` are considered as conflicting schemas and an exception would be
   *    thrown.
   */
  private[sql] def mergeStructs(left: StructType, right: StructType): StructType =
    merge(left, right).asInstanceOf[StructType]

  private[sql] def merge(left: DataType, right: DataType): DataType =
    mergeInternal(left, right, (s1: StructType, s2: StructType) => {
      val leftFields = s1.fields
      val rightFields = s2.fields
      val newFields = mutable.ArrayBuffer.empty[StructField]

      val rightMapped = fieldsMap(rightFields)
      leftFields.foreach {
        case leftField @ StructField(leftName, leftType, leftNullable, _) =>
          rightMapped.get(leftName)
            .map { case rightField @ StructField(rightName, rightType, rightNullable, _) =>
              try {
                leftField.copy(
                  dataType = merge(leftType, rightType),
                  nullable = leftNullable || rightNullable)
              } catch {
                case NonFatal(e) =>
                  throw QueryExecutionErrors.failedMergingFieldsError(leftName, rightName, e)
              }
            }
            .orElse {
              Some(leftField)
            }
            .foreach(newFields += _)
      }

      val leftMapped = fieldsMap(leftFields)
      rightFields
        .filterNot(f => leftMapped.get(f.name).nonEmpty)
        .foreach { f =>
          newFields += f
        }

      StructType(newFields.toArray)
    })

  private def mergeInternal(
      left: DataType,
      right: DataType,
      mergeStruct: (StructType, StructType) => StructType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, leftContainsNull),
      ArrayType(rightElementType, rightContainsNull)) =>
        ArrayType(
          mergeInternal(leftElementType, rightElementType, mergeStruct),
          leftContainsNull || rightContainsNull)

      case (MapType(leftKeyType, leftValueType, leftContainsNull),
      MapType(rightKeyType, rightValueType, rightContainsNull)) =>
        MapType(
          mergeInternal(leftKeyType, rightKeyType, mergeStruct),
          mergeInternal(leftValueType, rightValueType, mergeStruct),
          leftContainsNull || rightContainsNull)

      case (s1: StructType, s2: StructType) => mergeStruct(s1, s2)

      case (DecimalType.Fixed(leftPrecision, leftScale),
        DecimalType.Fixed(rightPrecision, rightScale)) =>
        if (leftScale == rightScale) {
          DecimalType(leftPrecision.max(rightPrecision), leftScale)
        } else {
          throw QueryExecutionErrors.cannotMergeDecimalTypesWithIncompatibleScaleError(
            leftScale, rightScale)
        }

      case (leftUdt: UserDefinedType[_], rightUdt: UserDefinedType[_])
        if leftUdt.userClass == rightUdt.userClass => leftUdt

      case (YearMonthIntervalType(lstart, lend), YearMonthIntervalType(rstart, rend)) =>
        YearMonthIntervalType(Math.min(lstart, rstart).toByte, Math.max(lend, rend).toByte)

      case (DayTimeIntervalType(lstart, lend), DayTimeIntervalType(rstart, rend)) =>
        DayTimeIntervalType(Math.min(lstart, rstart).toByte, Math.max(lend, rend).toByte)

      case (leftType, rightType) if leftType == rightType =>
        leftType

      case _ =>
        throw QueryExecutionErrors.cannotMergeIncompatibleDataTypesError(left, right)
    }

  private[sql] def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
    // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
    val map = mutable.Map[String, StructField]()
    map.sizeHint(fields.length)
    fields.foreach(s => map.put(s.name, s))
    map
  }

  private val SparkGeneratedName = """col\d+""".r
  private def isSparkGeneratedName(name: String): Boolean = name match {
    case SparkGeneratedName(_*) => true
    case _ => false
  }

  /**
   * Returns true if the write data type can be read using the read data type.
   *
   * The write type is compatible with the read type if:
   * - Both types are arrays, the array element types are compatible, and element nullability is
   *   compatible (read allows nulls or write does not contain nulls).
   * - Both types are maps and the map key and value types are compatible, and value nullability
   *   is compatible  (read allows nulls or write does not contain nulls).
   * - Both types are structs and have the same number of fields. The type and nullability of each
   *   field from read/write is compatible. If byName is true, the name of each field from
   *   read/write needs to be the same.
   * - Both types are atomic and the write type can be safely cast to the read type.
   *
   * Extra fields in write-side structs are not allowed to avoid accidentally writing data that
   * the read schema will not read, and to ensure map key equality is not changed when data is read.
   *
   * @param write a write-side data type to validate against the read type
   * @param read a read-side data type
   * @return true if data written with the write type can be read using the read type
   */
  def canWrite(
      write: DataType,
      read: DataType,
      byName: Boolean,
      resolver: Resolver,
      context: String,
      storeAssignmentPolicy: StoreAssignmentPolicy.Value,
      addError: String => Unit): Boolean = {
    (write, read) match {
      case (wArr: ArrayType, rArr: ArrayType) =>
        // run compatibility check first to produce all error messages
        val typesCompatible = canWrite(
          wArr.elementType, rArr.elementType, byName, resolver, context + ".element",
          storeAssignmentPolicy, addError)

        if (wArr.containsNull && !rArr.containsNull) {
          addError(s"Cannot write nullable elements to array of non-nulls: '$context'")
          false
        } else {
          typesCompatible
        }

      case (wMap: MapType, rMap: MapType) =>
        // map keys cannot include data fields not in the read schema without changing equality when
        // read. map keys can be missing fields as long as they are nullable in the read schema.

        // run compatibility check first to produce all error messages
        val keyCompatible = canWrite(
          wMap.keyType, rMap.keyType, byName, resolver, context + ".key",
          storeAssignmentPolicy, addError)
        val valueCompatible = canWrite(
          wMap.valueType, rMap.valueType, byName, resolver, context + ".value",
          storeAssignmentPolicy, addError)

        if (wMap.valueContainsNull && !rMap.valueContainsNull) {
          addError(s"Cannot write nullable values to map of non-nulls: '$context'")
          false
        } else {
          keyCompatible && valueCompatible
        }

      case (StructType(writeFields), StructType(readFields)) =>
        var fieldCompatible = true
        readFields.zip(writeFields).zipWithIndex.foreach {
          case ((rField, wField), i) =>
            val nameMatch = resolver(wField.name, rField.name) || isSparkGeneratedName(wField.name)
            val fieldContext = s"$context.${rField.name}"
            val typesCompatible = canWrite(
              wField.dataType, rField.dataType, byName, resolver, fieldContext,
              storeAssignmentPolicy, addError)

            if (byName && !nameMatch) {
              addError(s"Struct '$context' $i-th field name does not match " +
                s"(may be out of order): expected '${rField.name}', found '${wField.name}'")
              fieldCompatible = false
            } else if (!rField.nullable && wField.nullable) {
              addError(s"Cannot write nullable values to non-null field: '$fieldContext'")
              fieldCompatible = false
            } else if (!typesCompatible) {
              // errors are added in the recursive call to canWrite above
              fieldCompatible = false
            }
        }

        if (readFields.size > writeFields.size) {
          val missingFieldsStr = readFields.takeRight(readFields.size - writeFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          if (missingFieldsStr.nonEmpty) {
            addError(s"Struct '$context' missing fields: $missingFieldsStr")
            fieldCompatible = false
          }

        } else if (writeFields.size > readFields.size) {
          val extraFieldsStr = writeFields.takeRight(writeFields.size - readFields.size)
            .map(f => s"'${f.name}'").mkString(", ")
          addError(s"Cannot write extra fields to struct '$context': $extraFieldsStr")
          fieldCompatible = false
        }

        fieldCompatible

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == STRICT =>
        if (!Cast.canUpCast(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (_: NullType, _) if storeAssignmentPolicy == ANSI => true

      case (w: AtomicType, r: AtomicType) if storeAssignmentPolicy == ANSI =>
        if (!Cast.canANSIStoreAssign(w, r)) {
          addError(s"Cannot safely cast '$context': ${w.catalogString} to ${r.catalogString}")
          false
        } else {
          true
        }

      case (w, r) if w.sameType(r) && !w.isInstanceOf[NullType] =>
        true

      case (w, r) =>
        addError(s"Cannot write '$context': " +
          s"${w.catalogString} is incompatible with ${r.catalogString}")
        false
    }
  }
}
