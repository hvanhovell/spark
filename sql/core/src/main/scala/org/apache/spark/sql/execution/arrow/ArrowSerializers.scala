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
package org.apache.spark.sql.execution.arrow

import java.io.ByteArrayOutputStream
import java.lang.invoke.{MethodHandles, MethodType}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.nio.channels.Channels
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, DurationVector, FieldVector, Float4Vector, Float8Vector, IntervalYearVector, IntVector, NullVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.util.Text

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.util.ArrowUtils



// TODO improve null checking
// TODO improve type validation
object ArrowSerializers {
  import ArrowEncoderUtils._

  /**
   * Create an [[Iterator]] that converts the input [[Iterator]] of type `T`
   * into an [[Iterator]] of Arrow IPC Streams.
   */
  def serializeToArrow[T](
      input: Iterator[T],
      enc: AgnosticEncoder[T],
      allocator: BufferAllocator,
      maxRecordsPerBatch: Int,
      maxBatchSize: Long,
      timeZoneId: String,
      batchSizeCheckInterval: Int = 128): CloseableIterator[Array[Byte]] = {
    new CloseableIterator[Array[Byte]] {
      private val (root, serializer) = ArrowSerializers.serializerFor(enc, allocator, timeZoneId)
      private val vectors = root.getFieldVectors.asScala
      private val unloader = new VectorUnloader(root)
      private val bytes = new ByteArrayOutputStream
      private val channel = new WriteChannel(Channels.newChannel(bytes))
      private var hasWrittenFirstBatch = false

      private def resetVectorRoot(): Unit = vectors.foreach(_.reset())

      /**
       * Periodical check to make sure we don't go over the size threshold by too much.
       */
      private def sizeOk(i: Int): Boolean = {
        if (i > 0 && i % batchSizeCheckInterval == 0) {
          root.setRowCount(i)
          val sizeInBytes = vectors.map(_.getBufferSize).sum
          return sizeInBytes < maxBatchSize
        }
        true
      }

      override def hasNext: Boolean = input.hasNext || !hasWrittenFirstBatch

      override def next(): Array[Byte] = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        resetVectorRoot()
        bytes.reset()
        MessageSerializer.serialize(channel, root.getSchema)
        var i = 0
        while (i < maxRecordsPerBatch && input.hasNext && sizeOk(i)) {
          serializer.write(i, input.next())
          i += 1
        }
        root.setRowCount(i)
        val batch = unloader.getRecordBatch
        try MessageSerializer.serialize(channel, batch) finally {
          batch.close()
        }
        ArrowStreamWriter.writeEndOfStream(channel, IpcOption.DEFAULT)
        hasWrittenFirstBatch = true
        bytes.toByteArray
      }

      override def close(): Unit = root.close()
    }
  }

  /**
   * Create a (root) [[Serializer]] for [[AgnosticEncoder]] `encoder`.
   *
   * The serializer returned by this method is NOT thread-safe.
   */
  def serializerFor[T](
      encoder: AgnosticEncoder[T],
      allocator: BufferAllocator,
      timeZoneId: String): (VectorSchemaRoot, Serializer) = {
    val arrowSchema = ArrowUtils.toArrowSchema(encoder.schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val serializer = if (encoder.schema != encoder.dataType) {
      assert(root.getSchema.getFields.size() == 1)
      serializerFor(encoder, root.getVector(0))
    } else {
      serializerFor(encoder, root)
    }
    root -> serializer
  }

  private def serializerFor[E](encoder: AgnosticEncoder[E], v: AnyRef): Serializer = {
    (encoder, v) match {
      case (PrimitiveBooleanEncoder | BoxedBooleanEncoder, v: BitVector) =>
        new FieldSerializer[Boolean, BitVector](v) {
          override def set(index: Int, value: Boolean): Unit =
            vector.setSafe(index, if (value) 1 else 0)
        }
      case (PrimitiveByteEncoder | BoxedByteEncoder, v: TinyIntVector) =>
        new FieldSerializer[Byte, TinyIntVector](v) {
          override def set(index: Int, value: Byte): Unit = vector.setSafe(index, value)
        }
      case (PrimitiveShortEncoder | BoxedShortEncoder, v: SmallIntVector) =>
        new FieldSerializer[Short, SmallIntVector](v) {
          override def set(index: Int, value: Short): Unit = vector.setSafe(index, value)
        }
      case (PrimitiveIntEncoder | BoxedIntEncoder, v: IntVector) =>
        new FieldSerializer[Int, IntVector](v) {
          override def set(index: Int, value: Int): Unit = vector.setSafe(index, value)
        }
      case (PrimitiveLongEncoder | BoxedLongEncoder, v: BigIntVector) =>
        new FieldSerializer[Long, BigIntVector](v) {
          override def set(index: Int, value: Long): Unit = vector.setSafe(index, value)
        }
      case (PrimitiveFloatEncoder | BoxedFloatEncoder, v: Float4Vector) =>
        new FieldSerializer[Float, Float4Vector](v) {
          override def set(index: Int, value: Float): Unit = vector.setSafe(index, value)
        }
      case (PrimitiveDoubleEncoder | BoxedDoubleEncoder, v: Float8Vector) =>
        new FieldSerializer[Double, Float8Vector](v) {
          override def set(index: Int, value: Double): Unit = vector.setSafe(index, value)
        }
      case (NullEncoder, v: NullVector) =>
        new FieldSerializer[Unit, NullVector](v) {
          override def set(index: Int, value: Unit): Unit = ()
        }
      case (StringEncoder, v: VarCharVector) =>
        new FieldSerializer[String, VarCharVector](v) {
          override def set(index: Int, value: String): Unit = setString(v, index, value)
        }
      case (JavaEnumEncoder(_), v: VarCharVector) =>
        new FieldSerializer[Enum[_], VarCharVector](v) {
          override def set(index: Int, value: Enum[_]): Unit = setString(v, index, value.name())
        }
      case (ScalaEnumEncoder(_, _), v: VarCharVector) =>
        new FieldSerializer[Enumeration#Value, VarCharVector](v) {
          override def set(index: Int, value: Enumeration#Value): Unit =
            setString(v, index, value.toString)
        }
      case (BinaryEncoder, v: VarBinaryVector) =>
        new FieldSerializer[Array[Byte], VarBinaryVector](v) {
          override def set(index: Int, value: Array[Byte]): Unit = vector.setSafe(index, value)
        }
      case (SparkDecimalEncoder(_), v: DecimalVector) =>
        new FieldSerializer[Decimal, DecimalVector](v) {
          override def set(index: Int, value: Decimal): Unit =
            setDecimal(vector, index, value.toJavaBigDecimal)
        }
      case (ScalaDecimalEncoder(_), v: DecimalVector) =>
        new FieldSerializer[BigDecimal, DecimalVector](v) {
          override def set(index: Int, value: BigDecimal): Unit =
            setDecimal(vector, index, value.bigDecimal)
        }
      case (JavaDecimalEncoder(_, false), v: DecimalVector) =>
        new FieldSerializer[JBigDecimal, DecimalVector](v) {
          override def set(index: Int, value: JBigDecimal): Unit =
            setDecimal(vector, index, value)
        }
      case (JavaDecimalEncoder(_, true), v: DecimalVector) =>
        new FieldSerializer[Any, DecimalVector](v) {
          override def set(index: Int, value: Any): Unit = {
            val decimal = value match {
              case j: JBigDecimal => j
              case d: BigDecimal => d.bigDecimal
              case k: BigInt => new JBigDecimal(k.bigInteger)
              case l: JBigInteger => new JBigDecimal(l)
              case d: Decimal => d.toJavaBigDecimal
            }
            setDecimal(vector, index, decimal)
          }
        }
      case (ScalaBigIntEncoder, v: DecimalVector) =>
        new FieldSerializer[BigInt, DecimalVector](v) {
          override def set(index: Int, value: BigInt): Unit =
            setDecimal(vector, index, new JBigDecimal(value.bigInteger))
        }
      case (JavaBigIntEncoder, v: DecimalVector) =>
        new FieldSerializer[JBigInteger, DecimalVector](v) {
          override def set(index: Int, value: JBigInteger): Unit =
            setDecimal(vector, index, new JBigDecimal(value))
        }
      case (DayTimeIntervalEncoder, v: DurationVector) =>
        new FieldSerializer[Duration, DurationVector](v) {
          override def set(index: Int, value: Duration): Unit =
            vector.setSafe(index, IntervalUtils.durationToMicros(value))
        }
      case (YearMonthIntervalEncoder, v: IntervalYearVector) =>
        new FieldSerializer[Period, IntervalYearVector](v) {
          override def set(index: Int, value: Period): Unit =
            vector.setSafe(index, IntervalUtils.periodToMonths(value))
        }
      case (DateEncoder(true) | LocalDateEncoder(true), v: DateDayVector) =>
        new FieldSerializer[Any, DateDayVector](v) {
          override def set(index: Int, value: Any): Unit =
            vector.setSafe(index, DateTimeUtils.anyToDays(value))
        }
      case (DateEncoder(false), v: DateDayVector) =>
        new FieldSerializer[java.sql.Date, DateDayVector](v) {
          override def set(index: Int, value: java.sql.Date): Unit =
            vector.setSafe(index, DateTimeUtils.fromJavaDate(value))
        }
      case (LocalDateEncoder(false), v: DateDayVector) =>
        new FieldSerializer[LocalDate, DateDayVector](v) {
          override def set(index: Int, value: LocalDate): Unit =
            vector.setSafe(index, DateTimeUtils.localDateToDays(value))
        }
      case (TimestampEncoder(true) | InstantEncoder(true), v: TimeStampMicroTZVector) =>
        new FieldSerializer[Any, TimeStampMicroTZVector](v) {
          override def set(index: Int, value: Any): Unit =
            vector.setSafe(index, DateTimeUtils.anyToMicros(value))
        }
      case (TimestampEncoder(false), v: TimeStampMicroTZVector) =>
        new FieldSerializer[java.sql.Timestamp, TimeStampMicroTZVector](v) {
          override def set(index: Int, value: java.sql.Timestamp): Unit =
            vector.setSafe(index, DateTimeUtils.fromJavaTimestamp(value))
        }
      case (InstantEncoder(false), v: TimeStampMicroTZVector) =>
        new FieldSerializer[Instant, TimeStampMicroTZVector](v) {
          override def set(index: Int, value: Instant): Unit =
            vector.setSafe(index, DateTimeUtils.instantToMicros(value))
        }
      case (LocalDateTimeEncoder, v: TimeStampMicroVector) =>
        new FieldSerializer[LocalDateTime, TimeStampMicroVector](v) {
          override def set(index: Int, value: LocalDateTime): Unit =
            vector.setSafe(index, DateTimeUtils.localDateTimeToMicros(value))
        }

      case (OptionEncoder(value), v) =>
        new Serializer {
          private[this] val delegate: Serializer = serializerFor(value, v)
          override def write(index: Int, value: Any): Unit =
            delegate.write(index, value.asInstanceOf[Option[Any]].orNull)
        }

      case (ArrayEncoder(element, _), v: ListVector) =>
        val elementSerializer = serializerFor(element, v.getDataVector)
        val toIterator = (v: Array[Any]) => v.toIterator
        new ArraySerializer(v, toIterator, elementSerializer)

      case (IterableEncoder(tag, element, _, lenient), v: ListVector) =>
        val elementSerializer = serializerFor(element, v.getDataVector)
        val toIterator: Any => Iterator[_] = if (lenient) {
          {
            case i: scala.collection.Iterable[_] => i.toIterator
            case l: java.util.List[_] => l.iterator().asScala
            case a: Array[_] => a.iterator
            case o => unsupportedCollectionType(o.getClass)
          }
        } else if (isSubClass(Classes.ITERABLE, tag)) {
          v => v.asInstanceOf[scala.collection.Iterable[_]].toIterator
        } else if (isSubClass(Classes.JLIST, tag)) {
          v => v.asInstanceOf[java.util.List[_]].iterator().asScala
        } else {
          unsupportedCollectionType(tag.runtimeClass)
        }
        new ArraySerializer(v, toIterator, elementSerializer)

      case (MapEncoder(tag, key, value, _), v: MapVector) =>
        val structVector = v.getDataVector.asInstanceOf[StructVector]
        val keySerializer = serializerFor(key, structVector.getChild(MapVector.KEY_NAME))
        val valueSerializer = serializerFor(value, structVector.getChild(MapVector.VALUE_NAME))
        val extractor = if (isSubClass(classOf[scala.collection.Map[_, _]], tag)) {
          (v: Any) => v.asInstanceOf[scala.collection.Map[_, _]].iterator
        } else if (isSubClass(classOf[JMap[_, _]], tag)) {
          (v: Any) => v.asInstanceOf[JMap[Any, Any]].asScala.iterator
        } else {
          unsupportedCollectionType(tag.runtimeClass)
        }
        val keyFieldSer = new StructFieldSerializer[(Any, Any)](_._1, keySerializer)
        val valueFieldSer = new StructFieldSerializer[(Any, Any)](_._2, valueSerializer)
        val structSer = new StructSerializer(structVector, keyFieldSer :: valueFieldSer :: Nil)
        new ArraySerializer(v, extractor, structSer)

      case (ProductEncoder(tag, fields), StructVectors(struct, vectors)) =>
        if (isSubClass(classOf[Product], tag)) {
          structSerializerFor(fields, struct, vectors) { (_, i) =>
            p => p.asInstanceOf[Product].productElement(i)
          }
        } else if (isSubClass(classOf[DefinedByConstructorParams], tag)) {
          structSerializerFor(fields, struct, vectors) { (field, _) =>
            val getter = methodLookup.findVirtual(
              tag.runtimeClass,
              field.name,
              MethodType.methodType(field.enc.clsTag.runtimeClass))
            o => getter.invokeExact(o)
          }
        } else {
          unsupportedCollectionType(tag.runtimeClass)
        }

      case (RowEncoder(fields), StructVectors(struct, vectors)) =>
        structSerializerFor(fields, struct, vectors) { (_, i) =>
          r => r.asInstanceOf[Row].get(i)
        }

      case (JavaBeanEncoder(tag, fields), StructVectors(struct, vectors)) =>
        structSerializerFor(fields, struct, vectors) { (field, _) =>
          val getter = methodLookup.findGetter(
            tag.runtimeClass,
            field.name,
            field.enc.clsTag.runtimeClass)
          o => getter.invokeExact(o)
        }

      case (CalendarIntervalEncoder | _: UDTEncoder[_], _) =>
        throw QueryExecutionErrors.unsupportedDataTypeError(encoder.dataType.catalogString)

      case _ =>
        throw new RuntimeException(s"Unsupported Encoder($encoder)/Vector($v) combination.")
    }
  }

  private val methodLookup = MethodHandles.lookup()

  private def setString(vector: VarCharVector, index: Int, string: String): Unit = {
    val bytes = Text.encode(string)
    vector.setSafe(index, bytes, 0, bytes.limit())
  }

  private def setDecimal(vector: DecimalVector, index: Int, decimal: JBigDecimal): Unit = {
    val scaledDecimal = if (vector.getScale != decimal.scale()) {
      decimal.setScale(vector.getScale)
    } else {
      decimal
    }
    vector.setSafe(index, scaledDecimal)
  }

  private def structSerializerFor[S](
      fields: Seq[EncoderField],
      struct: StructVector,
      vectors: Seq[FieldVector])(
      createGetter: (EncoderField, Int) => Any => Any): StructSerializer[S] = {
    require(fields.size == vectors.size)
    val serializers = fields.zip(vectors).zipWithIndex.map { case ((field, vector), i) =>
      val serializer = serializerFor(field.enc, vector)
      new StructFieldSerializer[S](createGetter(field, i), serializer)
    }
    new StructSerializer(struct, serializers)
  }

  abstract class Serializer {
    def write(index: Int, value: Any): Unit
  }

  private abstract class FieldSerializer[E, V <: FieldVector](val vector: V) extends Serializer {
    private[this] val nullable = vector.getField.isNullable
    def set(index: Int, value: E): Unit

    override def write(index: Int, raw: Any): Unit = {
      val value = raw.asInstanceOf[E]
      if (value != null) {
        set(index, value)
      } else if (nullable) {
        vector.setNull(index)
      } else {
        throw new NullPointerException()
      }
    }
  }

  private class ArraySerializer[C](
      v: ListVector,
      toIterator: C => Iterator[Any],
      elementSerializer: Serializer)
    extends FieldSerializer[C, ListVector](v) {
    override def set(index: Int, value: C): Unit = {
      val elementStartIndex = vector.startNewValue(index)
      var elementIndex = elementStartIndex
      val iterator = toIterator(value)
      while (iterator.hasNext) {
        elementSerializer.write(elementIndex, iterator.next())
        elementIndex += 1
      }
      vector.endValue(index, elementIndex - elementStartIndex)
    }
  }

  private class StructFieldSerializer[S](
      val extractor: S => Any,
      val serializer: Serializer) {
    def write(index: Int, value: S): Unit = serializer.write(index, extractor(value))
  }

  private class StructSerializer[S](
      struct: StructVector,
      fieldSerializers: Seq[StructFieldSerializer[S]])
    extends Serializer {

    override def write(index: Int, raw: Any): Unit = {
      val value = raw.asInstanceOf[S]
      fieldSerializers.foreach(_.write(index, value))
      if (struct != null) {
        if (value != null) {
          struct.setIndexDefined(index)
        } else {
          struct.setNull(index)
        }
      }
    }
  }
}
