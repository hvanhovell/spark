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

package org.apache.spark.network.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

/**
 * General utilities available in the network package. Many of these are sourced from Spark's
 * own Utils, just accessible within this package.
 */
public class JavaUtils {
  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   */
  public static final long DEFAULT_DRIVER_MEM_MB = 1024;

  /** Closes the given object, ignoring IOExceptions. */
  public static void closeQuietly(Closeable closeable) {
    org.apache.spark.util.JavaUtils.closeQuietly(closeable);
  }

  /** Returns a hash consistent with Spark's Utils.nonNegativeHash(). */
  public static int nonNegativeHash(Object obj) {
    return org.apache.spark.util.JavaUtils.nonNegativeHash(obj);
  }

  /**
   * Convert the given string to a byte buffer. The resulting buffer can be
   * converted back to the same string through {@link #bytesToString(ByteBuffer)}.
   */
  public static ByteBuffer stringToBytes(String s) {
    return Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8)).nioBuffer();
  }

  /**
   * Convert the given byte buffer to a string. The resulting string can be
   * converted back to the same byte buffer through {@link #stringToBytes(String)}.
   */
  public static String bytesToString(ByteBuffer b) {
    return Unpooled.wrappedBuffer(b).toString(StandardCharsets.UTF_8);
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file) throws IOException {
    org.apache.spark.util.JavaUtils.deleteRecursively(file);
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @param filter A filename filter that make sure only files / dirs with the satisfied filenames
   *               are deleted.
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file, FilenameFilter filter) throws IOException {
    org.apache.spark.util.JavaUtils.deleteRecursively(file, filter);
  }
  /**
   * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit.
   * The unit is also considered the default if the given string does not specify a unit.
   */
  public static long timeStringAs(String str, TimeUnit unit) {
    return org.apache.spark.util.JavaUtils.timeStringAs(str, unit);
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  public static long timeStringAsMs(String str) {
    return org.apache.spark.util.JavaUtils.timeStringAsMs(str);
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  public static long timeStringAsSec(String str) {
    return org.apache.spark.util.JavaUtils.timeStringAsSec(str);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is
   * provided, a direct conversion to the provided unit is attempted.
   */
  public static long byteStringAs(String str, ByteUnit unit) {
    return org.apache.spark.util.JavaUtils.byteStringAs(str, unit);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  public static long byteStringAsBytes(String str) {
    return org.apache.spark.util.JavaUtils.byteStringAsBytes(str);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  public static long byteStringAsKb(String str) {
    return org.apache.spark.util.JavaUtils.byteStringAsKb(str);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  public static long byteStringAsMb(String str) {
    return org.apache.spark.util.JavaUtils.byteStringAsMb(str);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  public static long byteStringAsGb(String str) {
    return org.apache.spark.util.JavaUtils.byteStringAsGb(str);
  }

  /**
   * Returns a byte array with the buffer's contents, trying to avoid copying the data if
   * possible.
   */
  public static byte[] bufferToArray(ByteBuffer buffer) {
    return org.apache.spark.util.JavaUtils.bufferToArray(buffer);
  }

  /**
   * Create a temporary directory inside `java.io.tmpdir` with default namePrefix "spark".
   * The directory will be automatically deleted when the VM shuts down.
   */
  public static File createTempDir() throws IOException {
    return org.apache.spark.util.JavaUtils.createTempDir();
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  public static File createTempDir(String root, String namePrefix) throws IOException {
    return org.apache.spark.util.JavaUtils.createTempDir(root, namePrefix);
  }

  /**
   * Create a directory inside the given parent directory with default namePrefix "spark".
   * The directory is guaranteed to be newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root) throws IOException {
    return org.apache.spark.util.JavaUtils.createDirectory(root);
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root, String namePrefix) throws IOException {
    return org.apache.spark.util.JavaUtils.createDirectory(root, namePrefix);
  }

  /**
   * Fills a buffer with data read from the channel.
   */
  public static void readFully(ReadableByteChannel channel, ByteBuffer dst) throws IOException {
    org.apache.spark.util.JavaUtils.readFully(channel, dst);
  }
}
