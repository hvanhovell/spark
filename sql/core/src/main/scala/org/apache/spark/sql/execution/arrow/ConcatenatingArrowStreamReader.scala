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

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.ipc.{ArrowReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.{MessageChannelReader, MessageResult, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Schema

/**
 * [[ArrowReader]] that takes an [[Iterator]] of separate Arrow IPC Streams as its input. The
 * reader concatenates these individual streams.
 */
class ConcatenatingArrowStreamReader(
    allocator: BufferAllocator,
    input: Iterator[Array[Byte]])
  extends ArrowReader(allocator) {

  private[this] var _bytesRead: Long = 0
  private[this] var reader: MessageChannelReader = _

  private def nextMessageResult(): MessageResult = {
    var message = if (reader != null) {
      reader.readNext()
    } else {
      null
    }
    while (message == null && input.hasNext) {
      val bytes = input.next()
      _bytesRead += bytes.length
      if (bytes != null && bytes.length > 0) {
        val in = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))
        reader = new MessageChannelReader(in, allocator)
        message = reader.readNext()
      }
    }
    message
  }

  override def readSchema(): Schema = {
    val result = nextMessageResult()
    require(result != null, "Unexpected end of input. Missing schema.")
    MessageSerializer.deserializeSchema(result.getMessage)
  }

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()
    // Keep looping until we load a non-empty batch or until we exhaust the input.
    while (true) {
      val result = nextMessageResult()
      if (result == null) {
        return false
      }
      result.getMessage.headerType() match {
        case MessageHeader.RecordBatch =>
          loadRecordBatch(MessageSerializer.deserializeRecordBatch(
            result.getMessage,
            bodyBuffer(result)))
          if (getVectorSchemaRoot.getRowCount > 0) {
            return true
          }

        case MessageHeader.DictionaryBatch =>
          loadDictionary(MessageSerializer.deserializeDictionaryBatch(
            result.getMessage,
            bodyBuffer(result)))

        case MessageHeader.Schema =>
          val newSchema = MessageSerializer.deserializeSchema(result.getMessage)
          if (newSchema != getVectorSchemaRoot.getSchema) {
            throw new IllegalStateException("Mismatch between schemas")
          }
      }
    }
    false
  }

  private def bodyBuffer(result: MessageResult): ArrowBuf = {
    var buffer = result.getBodyBuffer
    if (buffer == null) {
      buffer = allocator.getEmpty
    }
    buffer
  }

  override def bytesRead(): Long = _bytesRead
  override def closeReadSource(): Unit = ()
}
