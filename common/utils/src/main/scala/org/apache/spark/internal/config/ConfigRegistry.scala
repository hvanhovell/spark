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
package org.apache.spark.internal.config

import java.util

import scala.collection.JavaConverters._

trait ConfigRegistry {

  /**
   * Check if a configuration is registered. It is up to the
   * implementation to throw an exception or not.
   *
   * @param key to check.
   */
  def checkEntryExists(key: String): Unit

  /**
   * Get the configuration entry associated with key `key`.
   *
   * @param key to get the entry for.
   * @return the entry associated with the key, null if there is no entry defined for the key.
   */
  def getConfigEntry(key: String): ConfigEntry[_]

  /**
   * Return all entries registered.
   */
  def allRegisteredEntries: Seq[ConfigEntry[_]]

  def getDeprecatedConfig(key: String): Option[DeprecatedConfig]

  def getRemovedConfig(key: String): Option[RemovedConfig]
}

/**
 * Holds information about keys that have been deprecated.
 *
 * @param key The deprecated key.
 * @param version Version of Spark where key was deprecated.
 * @param comment Additional info regarding to the removed config. For example,
 *                reasons of config deprecation, what users should use instead of it.
 */
case class DeprecatedConfig(key: String, version: String, comment: String)

/**
 * Holds information about keys that have been removed.
 *
 * @param key The removed config key.
 * @param version Version of Spark where key was removed.
 * @param defaultValue The default config value. It can be used to notice
 *                     users that they set non-default value to an already removed config.
 * @param comment Additional info regarding to the removed config.
 */
case class RemovedConfig(key: String, version: String, defaultValue: String, comment: String)

class NoopConfigRegistry extends ConfigRegistry {
  override def checkEntryExists(key: String): Unit = ()
  override def getConfigEntry(key: String): ConfigEntry[_] = null
  override def allRegisteredEntries: Seq[ConfigEntry[_]] = Nil
  override def getDeprecatedConfig(key: String): Option[DeprecatedConfig] = None
  override def getRemovedConfig(key: String): Option[RemovedConfig] = None
}

class SimpleConfigRegistry extends ConfigRegistry {
  private[this] val entriesUpdateLock = new Object

  @volatile
  private[this] var entries: util.Map[String, ConfigEntry[_]] = util.Collections.emptyMap()

  protected def register(entry: ConfigEntry[_]): Unit = entriesUpdateLock.synchronized {
    require(!entries.containsKey(entry.key),
      s"Duplicate Entry. ${entry.key} has been registered")
    val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](entries)
    updatedMap.put(entry.key, entry)
    entries = updatedMap
  }

  protected def unregister(entry: ConfigEntry[_]): Unit = entriesUpdateLock.synchronized {
    val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](entries)
    updatedMap.remove(entry.key)
    entries = updatedMap
  }

  protected def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  protected def getConf[E](entry: ConfigEntry[E]): E = Config.get.getConf(entry)

  override def checkEntryExists(key: String): Unit = entries.containsKey(key)

  override def getConfigEntry(key: String): ConfigEntry[_] = entries.get(key)

  override def allRegisteredEntries: Seq[ConfigEntry[_]] = entries.values().asScala.toSeq

  override def getDeprecatedConfig(key: String): Option[DeprecatedConfig] = None

  override def getRemovedConfig(key: String): Option[RemovedConfig] = None
}
