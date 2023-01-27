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

import java.util
import java.util.{Collections, Objects}

import scala.collection.mutable

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, DummyBean, FooEnum, JavaTypeInference, PrimitiveData, ScalaReflection}
import org.apache.spark.sql.catalyst.FooEnum.FooEnum
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, BoxedData, UDTForCaseClass}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BinaryEncoder, BoxedIntEncoder, CalendarIntervalEncoder, EncoderField, RowEncoder, StringEncoder, UDTEncoder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder.{encoderFor => toRowEncoder}
import org.apache.spark.sql.types.{ArrayType, Decimal, DecimalType, Metadata, StructType}

class ArrowEncoderSuite extends SparkFunSuite {
  private val allocator = new RootAllocator()

  private def newAllocator(name: String): BufferAllocator = {
    allocator.newChildAllocator(name, 0, allocator.getLimit)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    allocator.close()
  }

  private def withAllocator[T](f: BufferAllocator => T): T = {
    val allocator = newAllocator("allocator")
    try f(allocator) finally {
      allocator.close()
    }
  }

  private def roundTrip[T](
      encoder: AgnosticEncoder[T],
      iterator: Iterator[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null): CloseableIterator[T] = {

    // Use different allocators so we can pinpoint memory leaks better.
    val serializerAllocator = newAllocator("serialization")
    val deserializerAllocator = newAllocator("deserialization")

    val arrowIterator = ArrowSerializers.serializeToArrow(
      input = iterator,
      enc = encoder,
      allocator = serializerAllocator,
      maxRecordsPerBatch = maxRecordsPerBatch,
      maxBatchSize = maxBatchSize,
      batchSizeCheckInterval = batchSizeCheckInterval,
      timeZoneId = "UTC")

    val inspectedIterator = if (inspectBatch != null) {
      arrowIterator.map { batch =>
        inspectBatch(batch)
        batch
      }
    } else {
      arrowIterator
    }

    val resultIterator = ArrowDeserializers.deserializeFromArrow(
      inspectedIterator,
      encoder,
      deserializerAllocator)
    new CloseableIterator[T] {
      override def close(): Unit = {
        arrowIterator.close()
        resultIterator.close()
        serializerAllocator.close()
        deserializerAllocator.close()
      }
      override def hasNext: Boolean = resultIterator.hasNext
      override def next(): T = resultIterator.next()
    }
  }

  private def roundTripAndCheck[T](
      encoder: AgnosticEncoder[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      batchSizeCheckInterval: Int = 128,
      inspectBatch: Array[Byte] => Unit = null)(
      toIterator: () => Iterator[T]): Unit = {
    val iterator = roundTrip(
      encoder,
      toIterator(),
      maxRecordsPerBatch,
      maxBatchSize,
      batchSizeCheckInterval,
      inspectBatch)
    try {
      compareIterators(toIterator(), iterator)
    } finally {
      iterator.close()
    }
  }

  private def serializeToArrow[T](
      input: Iterator[T],
      encoder: AgnosticEncoder[T],
      allocator: BufferAllocator): CloseableIterator[Array[Byte]] = {
    ArrowSerializers.serializeToArrow(
      input,
      encoder,
      allocator,
      maxRecordsPerBatch = 1024,
      maxBatchSize = 8 * 1024,
      timeZoneId = "UTC")
  }

  private def compareIterators[T](expected: Iterator[T], actual: Iterator[T]): Unit = {
    expected.zipAll(actual, null, null).foreach {
      case (expected, actual) =>
        assert(expected != null)
        assert(actual != null)
        assert(actual == expected)
    }
  }

  private class CountingBatchInspector extends (Array[Byte] => Unit) {
    private var _numBatches: Int = 0
    private var _sizeInBytes: Long = 0
    def numBatches: Int = _numBatches
    def sizeInBytes: Long = _sizeInBytes
    def sizeInBytesPerBatch: Long = sizeInBytes / numBatches
    override def apply(batch: Array[Byte]): Unit = {
      _numBatches += 1
      _sizeInBytes += batch.length
    }
  }

  private case class MaybeNull(interval: Int) {
    assert(interval > 1)
    private var invocations = 0
    def apply[T](value: T): T = {
      val result = if (invocations % interval == 0) {
        null.asInstanceOf[T]
      } else {
        value
      }
      invocations += 1
      result
    }
  }

  private def javaBigDecimal(i: Int): java.math.BigDecimal = {
    javaBigDecimal(i, DecimalType.DEFAULT_SCALE)
  }

  private def javaBigDecimal(i: Int, scale: Int): java.math.BigDecimal = {
    java.math.BigDecimal.valueOf(i).setScale(scale)
  }

  private val singleIntEncoder = RowEncoder(
    EncoderField("i", BoxedIntEncoder, nullable = false, Metadata.empty) :: Nil)

  /* ******************************************************************** *
   * Iterator behavior tests.
   * ******************************************************************** */

  test("empty") {
    val inspector = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.empty
    }
    // We always write a batch with a schema.
    assert(inspector.numBatches == 1)
    assert(inspector.sizeInBytes > 0)
  }

  test("deserializing empty iterator") {
    withAllocator { allocator =>
      val iterator = ArrowDeserializers.deserializeFromArrow(
        Iterator.empty,
        singleIntEncoder,
        allocator)
      assert(iterator.isEmpty)
      assert(iterator.encoder == singleIntEncoder)
      assert(allocator.getAllocatedMemory == 0)

      val rowIterator = ArrowDeserializers.deserializeFromArrow(Iterator.empty, allocator)
      assert(rowIterator.isEmpty)
      assert(rowIterator.encoder == RowEncoder(Nil))
      assert(allocator.getAllocatedMemory == 0)
    }
  }

  test("single batch") {
    val inspector = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.tabulate(10)(i => Row(i))
    }
    assert(inspector.numBatches == 1)
  }

  test("multiple batches - split by record count") {
    val inspector = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, inspectBatch = inspector, maxBatchSize = 32 * 1024) { () =>
      Iterator.tabulate(1024 * 1024)(i => Row(i))
    }
    assert(inspector.numBatches == 256)
  }

  test("multiple batches - split by size") {
    val dataGen = { () =>
      Iterator.tabulate(4 * 1024)(i => Row(i))
    }

    // Normal interval
    val inspector1 = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, maxBatchSize = 1024, inspectBatch = inspector1)(dataGen)
    assert(inspector1.numBatches == 16)
    assert(inspector1.sizeInBytesPerBatch >= 1024)
    assert(inspector1.sizeInBytesPerBatch <= 1024 + 128 * 5)

    // Lowest possible interval
    val inspector2 = new CountingBatchInspector
    roundTripAndCheck(
      singleIntEncoder,
      maxBatchSize = 1024,
      batchSizeCheckInterval = 1,
      inspectBatch = inspector2)(
      dataGen)
    assert(inspector2.numBatches == 20)
    assert(inspector2.sizeInBytesPerBatch >= 1024)
    assert(inspector2.sizeInBytesPerBatch <= 1024 + 128 * 2)
    assert(inspector2.sizeInBytesPerBatch < inspector1.sizeInBytesPerBatch)
  }

  test("row iterator") {
    withAllocator { allocator =>
      def genData(): Iterator[(Int, String)] = {
        val maybeNull = MaybeNull(5)
        Iterator.tabulate(123)(i => i -> maybeNull("v" + 1))
      }
      val encoder = ScalaReflection.encoderFor[(Int, String)]
      val input = serializeToArrow(genData(), encoder, allocator)
      val rows = ArrowDeserializers.deserializeFromArrow(input, allocator)
      assert(encoder.schema === rows.schema)
      compareIterators(
        genData().map(t => Row(t.productIterator.toSeq: _*)),
        rows)
      rows.close()
      rows.close()
      input.close()
    }
  }

  /* ******************************************************************** *
   * Encoder specification tests
   * ******************************************************************** */
  // Maps
  // Lenient mode
  // Errors

  test("primitive fields") {
    val encoder = ScalaReflection.encoderFor[PrimitiveData]
    roundTripAndCheck(encoder) { () =>
      Iterator.tabulate(10) { i =>
        PrimitiveData(i, i, i.toDouble, i.toFloat, i.toShort, i.toByte, i < 4)
      }
    }
  }

  test("boxed primitive fields") {
    val encoder = ScalaReflection.encoderFor[BoxedData]
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        BoxedData(
          intField = maybeNull(i),
          longField = maybeNull(i),
          doubleField = maybeNull(i.toDouble),
          floatField = maybeNull(i.toFloat),
          shortField = maybeNull(i.toShort),
          byteField = maybeNull(i.toByte),
          booleanField = maybeNull(i > 4))
      }
    }
  }

  test("nullable fields") {
    val encoder = ScalaReflection.encoderFor[NullableData]
    val instant = java.time.Instant.now()
    val now = java.time.LocalDateTime.now()
    val today = java.time.LocalDate.now()
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        NullableData(
          nullable = null,
          string = maybeNull(if (i % 7 == 0) "" else "s" + i),
          month = maybeNull(java.time.Month.of(1 + (i % 12))),
          foo = maybeNull(FooEnum(i % FooEnum.maxId)),
          decimal = maybeNull(Decimal(i)),
          scalaBigDecimal = maybeNull(BigDecimal(javaBigDecimal(i + 1))),
          javaBigDecimal = maybeNull(javaBigDecimal(i + 2)),
          scalaBigInt = maybeNull(BigInt(i + 3)),
          javaBigInteger = maybeNull(java.math.BigInteger.valueOf(i + 4)),
          duration = maybeNull(java.time.Duration.ofDays(i)),
          period = maybeNull(java.time.Period.ofMonths(i)),
          date = maybeNull(java.sql.Date.valueOf(today.plusDays(i))),
          localDate = maybeNull(today.minusDays(i)),
          timestamp = maybeNull(java.sql.Timestamp.valueOf(now.plusSeconds(i))),
          instant = maybeNull(instant.plusSeconds(i * 100)),
          localDateTime = maybeNull(now.minusHours(i)))
      }
    }
  }

  test("binary field") {
    val encoder = ScalaReflection.encoderFor[BinaryData]
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(3)
      Iterator.tabulate(100) { i =>
        BinaryData(maybeNull(Array.tabulate(i % 100)(_.toByte)))
      }
    }
  }

  // Row and Scala class are already covered in other tests
  test("javabean") {
    val encoder = JavaTypeInference.encoderFor[DummyBean](classOf[DummyBean])
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(6)
      Iterator.tabulate(100) { i =>
        val bean = new DummyBean()
        bean.setBigInteger(maybeNull(java.math.BigInteger.valueOf(i)))
        bean
      }
    }
  }

  test("defined by constructor parameters") {
    val encoder = ScalaReflection.encoderFor[NonProduct]
    roundTripAndCheck(encoder) { () =>
      Iterator.tabulate(100) { i =>
        new NonProduct("k" + i, i.toDouble)
      }
    }
  }

  test("option") {
    val encoder = ScalaReflection.encoderFor[Option[String]]
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(6)
      Iterator.tabulate(100) { i =>
        Option(maybeNull("v" + i))
      }
    }
  }

  test("arrays") {
    val encoder = ScalaReflection.encoderFor[ArrayData]
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(5)
      Iterator.tabulate(100) { i =>
        ArrayData(
          maybeNull(Array.tabulate[Double](i % 9)(_.toDouble)),
          maybeNull(Array.tabulate[String](i % 21)(i => maybeNull("s" + i))),
          maybeNull(Array.tabulate[Array[Int]](i % 13) { i =>
            maybeNull {
              Array.fill(i % 29)(i)
            }
          })
        )
      }
    }
  }

  test("scala iterables") {
    val encoder = ScalaReflection.encoderFor[ListData]
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(5)
      Iterator.tabulate(100) { i =>
        ListData(
          maybeNull(Seq.tabulate[String](i % 9)(i => maybeNull("s" + i))),
          maybeNull(Seq.tabulate[Int](i % 10)(identity)),
          maybeNull(Set(i.toLong, i.toLong - 1, i.toLong - 33)),
          maybeNull(mutable.Queue.tabulate(5 + i % 6) { i =>
            Option(maybeNull(BigInt(i)))
          }))
      }
    }
  }

  test("java lists") {
    def genJavaData[E](n: Int, collection: util.Collection[E])(f: Int => E): Unit = {
      Iterator.tabulate(n)(f).foreach(collection.add)
    }
    val encoder = JavaTypeInference.encoderFor[JavaListData](classOf[JavaListData])
    roundTripAndCheck(encoder) { () =>
      val maybeNull = MaybeNull(7)
      Iterator.tabulate(100) { i =>
        val bean = new JavaListData
        bean.setListOfDecimal(maybeNull {
          val list = new util.ArrayList[Decimal]
          genJavaData(i % 7, list) { i => maybeNull(Decimal(i * 33)) }
          list
        })
        bean.setListOfBigInt(maybeNull{
          val list = new util.LinkedList[BigInt]
          genJavaData(10, list) { i => maybeNull(BigInt(i * 50)) }
          list
        })
        bean.setListOfStrings(maybeNull {
          val list = new util.ArrayList[String]
          genJavaData(i % 50, list) { i => maybeNull("v" + (i * 2)) }
          list
        })
        bean.setListOfBytes(maybeNull(Collections.singletonList(i.toByte)))
        bean
      }
    }
  }

  test("wrapped array") {
    val encoder = ScalaReflection.encoderFor[mutable.WrappedArray[Int]]
    val input = mutable.WrappedArray.make[Int](Array(1, 98, 7, 6))
    val iterator = roundTrip(encoder, Iterator.single(input))
    val Seq(result) = iterator.toSeq
    assert(result == input)
    assert(result.array.getClass == classOf[Array[Int]])
    iterator.close()
  }

  test("wrapped array - empty") {
    val schema = new StructType().add("names", "array<string>")
    val encoder = toRowEncoder(schema)
    val iterator = roundTrip(encoder, Iterator.single(Row(Seq())))
    val Seq(Row(raw)) = iterator.toSeq
    val seq = raw.asInstanceOf[mutable.WrappedArray[String]]
    assert(seq.isEmpty)
    assert(seq.array.getClass == classOf[Array[String]])
    iterator.close()
  }

  test("maps") {
    // Scala:
    // - Map
    // - HashMap
    // Java
    // - Map
    // - HashMap
  }

  test("lenient field serialization") {
    // Date
    // Timestamp
    // Decimal
    // Arrays
  }

  test("unsupported collections") {
    // Scala seq without companion
    // Class that doesn;t hit the match
    // Abstract java list that cannot be represented by j.u.ArrayList
    // - org.apache.commons.collections.AbstractLinkedList
    // Abstract java map that cannot be represented by j.u.HashMap
    // -
    // Class with non-standard constructor
    // Class with non-visible constructor
  }

  test("unsupported structs classes") {
    // Bad Bean
    // Bad Product
  }

  test("unsupported encoders") {
    // CalendarIntervalEncoder
    val data = null.asInstanceOf[AnyRef]
    intercept[SparkUnsupportedOperationException](
      ArrowSerializers.serializerFor(CalendarIntervalEncoder, data))
    intercept[SparkUnsupportedOperationException](
      ArrowDeserializers.deserializerFor(CalendarIntervalEncoder, data))

    // UDT
    val udtEncoder = UDTEncoder(new UDTForCaseClass, classOf[UDTForCaseClass])
    intercept[SparkUnsupportedOperationException](
      ArrowSerializers.serializerFor(udtEncoder, data))
    intercept[SparkUnsupportedOperationException](
      ArrowDeserializers.deserializerFor(udtEncoder, data))
  }

  test("unsupported encoder/vector combinations") {
    // Also add a test for the serializer...
    withAllocator { allocator =>
      val arrowBatches = serializeToArrow(Iterator("a", "b"), StringEncoder, allocator)
      intercept[RuntimeException] {
        ArrowDeserializers.deserializeFromArrow(arrowBatches, BinaryEncoder, allocator)
      }
      arrowBatches.close()
    }
  }

  private val wideSchemaEncoder = toRowEncoder(new StructType()
    .add("a", "int")
    .add("b", "string")
    .add("c", new StructType()
      .add("ca", "array<int>")
      .add("cb", "binary")
      .add("cc", "float"))
    .add("d", ArrayType(new StructType()
      .add("da", "decimal(20, 10)")
      .add("db", "string")
      .add("dc", "boolean"))))

  private val narrowSchemaEncoder = toRowEncoder(new StructType()
    .add("b", "string")
    .add("d", ArrayType(new StructType()
      .add("da", "decimal(20, 10)")
      .add("dc", "boolean")))
    .add("C", new StructType()
      .add("Ca", "array<int>")
      .add("Cb", "binary")))

  test("bind to schema") {
    // Binds to a wider schema. The narrow schema has fewer (nested) fields, has a slightly
    // different field order, and uses different cased names in a couple of places.
    withAllocator { allocator =>
      val input = Row(
        887,
        "foo",
        Row(Seq(1, 7, 5), Array[Byte](8.toByte, 756.toByte), 5f),
        Seq(Row(null, "a", false), Row(javaBigDecimal(57853, 10), "b", false)))
      val expected = Row(
        "foo",
        Seq(Row(null, false), Row(javaBigDecimal(57853, 10), false)),
        Row(Seq(1, 7, 5), Array[Byte](8.toByte, 756.toByte)))
      val arrowBatches = serializeToArrow(Iterator.single(input), wideSchemaEncoder, allocator)
      val result = ArrowDeserializers.deserializeFromArrow(
        arrowBatches,
        narrowSchemaEncoder,
        allocator)
      val actual = result.next()
      assert(result.isEmpty)
      assert(expected === actual)
      result.close()
      arrowBatches.close()
    }
  }

  test("unknown field") {
    withAllocator { allocator =>
      val arrowBatches = serializeToArrow(Iterator.empty, narrowSchemaEncoder, allocator)
      intercept[AnalysisException] {
        ArrowDeserializers.deserializeFromArrow(
          arrowBatches,
          wideSchemaEncoder,
          allocator)
      }
      arrowBatches.close()
    }
  }

  test("duplicate fields") {
    val duplicateSchemaEncoder = toRowEncoder(new StructType()
      .add("foO", "string")
      .add("Foo", "string"))
    val fooSchemaEncoder = toRowEncoder(new StructType()
      .add("foo", "string"))
    withAllocator { allocator =>
      val arrowBatches = serializeToArrow(Iterator.empty, duplicateSchemaEncoder, allocator)
      intercept[AnalysisException] {
        ArrowDeserializers.deserializeFromArrow(
          arrowBatches,
          fooSchemaEncoder,
          allocator)
      }
      arrowBatches.close()
    }
  }
}

case class NullableData(
    nullable: Null,
    string: String,
    month: java.time.Month,
    foo: FooEnum,
    decimal: Decimal,
    scalaBigDecimal: BigDecimal,
    javaBigDecimal: java.math.BigDecimal,
    scalaBigInt: BigInt,
    javaBigInteger: java.math.BigInteger,
    duration: java.time.Duration,
    period: java.time.Period,
    date: java.sql.Date,
    localDate: java.time.LocalDate,
    timestamp: java.sql.Timestamp,
    instant: java.time.Instant,
    localDateTime: java.time.LocalDateTime)

case class BinaryData(binary: Array[Byte]) {
  def canEqual(other: Any): Boolean = other.isInstanceOf[BinaryData]

  override def equals(other: Any): Boolean = other match {
    case that: BinaryData if that.canEqual(this) =>
      java.util.Arrays.equals(binary, that.binary)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(binary)
}

class NonProduct(val name: String, val value: Double) extends DefinedByConstructorParams {

  def canEqual(other: Any): Boolean = other.isInstanceOf[NonProduct]

  override def equals(other: Any): Boolean = other match {
    case that: NonProduct =>
      (that canEqual this) &&
        name == that.name &&
        value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

case class ArrayData(doubles: Array[Double], strings: Array[String], nested: Array[Array[Int]]) {
  def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayData]

  override def equals(other: Any): Boolean = other match {
    case that: ArrayData if that.canEqual(this) =>
      Objects.deepEquals(that.doubles, doubles) &&
        Objects.deepEquals(that.strings, strings) &&
        Objects.deepEquals(that.nested, nested)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(doubles, strings, nested)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

case class ListData(
    seqOfStrings: Seq[String],
    seqOfInts: Seq[Int],
    setOfLongs: Set[Long],
    queueOfBigIntOptions: mutable.Queue[Option[BigInt]])

class JavaListData {
  @scala.beans.BeanProperty
  var listOfDecimal: java.util.ArrayList[Decimal] = _
  @scala.beans.BeanProperty
  var listOfBigInt: java.util.LinkedList[BigInt] = _
  @scala.beans.BeanProperty
  var listOfStrings: java.util.AbstractList[String] = _
  @scala.beans.BeanProperty
  var listOfBytes: java.util.List[Byte] = _
}

case class MapData()
