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

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{NullableData, PrimitiveData, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, BoxedData}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BoxedIntEncoder, EncoderField, RowEncoder}
import org.apache.spark.sql.types.Metadata

// All leafs
// Arrays
// Maps
// Structs
// Lenient mode
// Errors
//
class ArrowEncoderSuite extends SparkFunSuite {
  private val allocator = new RootAllocator()

  protected override def afterAll(): Unit = {
    super.afterAll()
    allocator.close()
  }

  private def roundTrip[T](
      encoder: AgnosticEncoder[T],
      iterator: Iterator[T],
      maxRecordsPerBatch: Int = 4 * 1024,
      maxBatchSize: Long = 16 * 1024,
      inspectBatch: Array[Byte] => Unit = null): CloseableIterator[T] = {

    // Use different allocators so we can pinpoint memory leaks better.
    val serializerAllocator = allocator.newChildAllocator(
      "serialization", 0, allocator.getLimit)
    val deserializerAllocator = allocator.newChildAllocator(
      "deserialization", 0, allocator.getLimit)

    val arrowIterator = ArrowSerializers.serializeToArrow(
      input = iterator,
      enc = encoder,
      allocator = serializerAllocator,
      maxRecordsPerBatch = maxRecordsPerBatch,
      maxBatchSize = maxBatchSize,
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
      inspectBatch: Array[Byte] => Unit = null)(
      toIterator: () => Iterator[T]): Unit = {
    val iterator = roundTrip(
      encoder,
      toIterator(),
      maxRecordsPerBatch,
      maxBatchSize,
      inspectBatch)
    try {
      toIterator().zipAll(iterator, null, null).foreach {
        case (expected, actual) =>
          assert(expected != null)
          assert(actual != null)
          assert(actual == expected)
      }
    } finally {
      iterator.close()
    }
  }

  private class CountingBatchInspector extends (Array[Byte] => Unit) {
    private var _numBatches: Int = 0
    private var _sizeInBytes: Long = 0
    def numBatches: Int = _numBatches
    def sizeInBytes: Long = _sizeInBytes
    override def apply(batch: Array[Byte]): Unit = {
      _numBatches += 1
      _sizeInBytes += batch.length
    }
  }

  private def maybeNull()

  private val singleIntEncoder = RowEncoder(
    EncoderField("i", BoxedIntEncoder, nullable = false, Metadata.empty) :: Nil)

  test("empty") {
    val inspector = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.empty
    }
    // We always write a batch with a schema.
    assert(inspector.numBatches == 1)
    assert(inspector.sizeInBytes > 0)
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
    roundTripAndCheck(singleIntEncoder, inspectBatch = inspector) { () =>
      Iterator.tabulate(1024 * 1024)(i => Row(i))
    }
    assert(inspector.numBatches == 256)
  }

  test("multiple batches - split by size") {
    val inspector = new CountingBatchInspector
    roundTripAndCheck(singleIntEncoder, maxBatchSize = 1024, inspectBatch = inspector) { () =>
      Iterator.tabulate(4 * 1024)(i => Row(i))
    }
    assert(inspector.numBatches == 16)
    assert(inspector.sizeInBytes >= (15 * 1024))
  }

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
      var invocations = 0
      def maybeNull[T](value: T): T = {
        val result = if (invocations % 5 == 0) {
          null.asInstanceOf[T]
        } else {
          value
        }
        invocations += 1
        value
      }
      val now = System.currentTimeMillis()
      Iterator.tabulate(100) { i =>
        NullableData(
          intField = maybeNull(i),
          longField = maybeNull(i),
          doubleField = maybeNull(i.toDouble),
          floatField = maybeNull(i.toFloat),
          shortField = maybeNull(i.toShort),
          byteField = maybeNull(i.toByte),
          booleanField = maybeNull(i > 4),
          stringField = maybeNull("s" + i),
          decimalField = maybeNull(java.math.BigDecimal.valueOf(i)),
          dateField = maybeNull(new java.sql.Date(now + i))
        )
      }
    }
  }

  test("nullable fields") {

  }

  test("lenient field serialization") {

  }

  test("unsupported collections") {

  }

  test("unsupported encoders") {

  }
}
