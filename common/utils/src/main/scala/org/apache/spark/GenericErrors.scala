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
package org.apache.spark

trait GenericErrors {
  def unsupportedOperationExceptionError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2225",
      messageParameters = Map.empty)
  }

  def fieldIndexOnRowWithoutSchemaError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2231",
      messageParameters = Map.empty)
  }

  def valueIsNullError(index: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2232",
      messageParameters = Map(
        "index" -> java.lang.Integer.toString(index)),
      cause = null)
  }

  def nullLiteralsCannotBeCastedError(name: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2226",
      messageParameters = Map(
        "name" -> name))
  }

  def noSuchElementExceptionError(key: String): Throwable = {
    new NoSuchElementException(key)
  }

  def configRemovedInVersionError(
      configName: String,
      version: String,
      comment: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_1226",
      messageParameters = Map(
        "configName" -> configName,
        "version" -> version,
        "comment" -> comment),
      cause = null)
  }
}

object GenericErrors extends GenericErrors
