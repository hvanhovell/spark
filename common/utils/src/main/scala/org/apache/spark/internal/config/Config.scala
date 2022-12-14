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
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.spark.GenericErrors
import org.apache.spark.internal.Logging

class Config(registry: ConfigRegistry) extends Logging {
  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings: util.Map[String, String] =
    util.Collections.synchronizedMap(new util.HashMap)

  @transient protected val reader = new ConfigReader(settings)

  /** Set configuration properties. */
  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v) }
  }

  /** Set the given configuration property using a `string` value. */
  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = registry.getConfigEntry(key)
    if (entry != null) {
      // Only verify configs in the SQLConf object
      entry.valueConverter(value)
    }
    setConfWithCheck(key, value)
  }

  /** Set the given configuration property. */
  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    registry.checkEntryExists(entry.key)
    setConfWithCheck(entry.key, entry.stringConverter(value))
  }

  /** Return the value of the configuration property for the given key. */
  @throws[NoSuchElementException]("if key is not set")
  def getConfString(key: String): String = {
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(registry.getConfigEntry(key).asInstanceOf[ConfigEntry[Any]]).map { e =>
          e.stringConverter(e.readFrom(reader))
        }
      }.
      getOrElse(throw GenericErrors.noSuchElementExceptionError(key))
  }

  /**
   * Return the value of the configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
   * desired one.
   */
  def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
    registry.checkEntryExists(entry.key)
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  /**
   * Return the value of the configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  def getConf[T](entry: ConfigEntry[T]): T = {
    registry.checkEntryExists(entry.key)
    entry.readFrom(reader)
  }

  /**
   * Return the value of an optional Spark SQL configuration property for the given key. If the key
   * is not set yet, returns None.
   */
  def getConf[T](entry: OptionalConfigEntry[T]): Option[T] = {
    registry.checkEntryExists(entry.key)
    entry.readFrom(reader)
  }

  /**
   * Return the `string` value of Spark SQL configuration property for the given key. If the key is
   * not set yet, return `defaultValue`.
   */
  def getConfString(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse {
      // If the key is not set, need to check whether the config entry is registered and is
      // a fallback conf, so that we can check its parent.
      registry.getConfigEntry(key) match {
        case e: FallbackConfigEntry[_] =>
          getConfString(e.fallback.key, defaultValue)
        case e: ConfigEntry[_] if defaultValue != null && defaultValue != ConfigEntry.UNDEFINED =>
          // Only verify configs in the SQLConf object
          e.valueConverter(defaultValue)
          defaultValue
        case _ =>
          defaultValue
      }
    }
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }

  /**
   * Return all the configuration definitions that have been registered. Each
   * definition contains key, value, doc, and version.
   */
  def getAllDefinedConfs: Seq[(String, String, String, String)] = {
    registry.allRegisteredEntries.filter(_.isPublic).map { entry =>
      val displayValue = Option(getConfString(entry.key, null)).getOrElse(entry.defaultValueString)
      (entry.key, displayValue, entry.doc, entry.version)
    }
  }

  /**
   * Return whether a given key is set in this [[Config]].
   */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    registry.getDeprecatedConfig(key).foreach {
      case DeprecatedConfig(configName, version, comment) =>
        logWarning(
          s"The SQL config '$configName' has been deprecated in Spark v$version " +
            s"and may be removed in the future. $comment")
    }
  }

  private def requireDefaultValueOfRemovedConf(key: String, value: String): Unit = {
    registry.getRemovedConfig(key).foreach {
      case RemovedConfig(configName, version, defaultValue, comment) =>
        if (value != defaultValue) {
          throw GenericErrors.configRemovedInVersionError(configName, version, comment)
        }
    }
  }

  protected def setConfWithCheck(key: String, value: String): Unit = {
    logDeprecationWarning(key)
    requireDefaultValueOfRemovedConf(key, value)
    settings.put(key, value)
  }

  def unsetConf(key: String): Unit = {
    logDeprecationWarning(key)
    settings.remove(key)
  }

  def unsetConf(entry: ConfigEntry[_]): Unit = {
    unsetConf(entry.key)
  }

  def clear(): Unit = {
    settings.clear()
  }
}

object Config {
  /**
   * Fallback config. Only used when there is no getter defined.
   */
  private lazy val fallbackConf = new ThreadLocal[Config] {
    override def initialValue: Config = new Config(new NoopConfigRegistry)
  }

  private lazy val existingConf = new ThreadLocal[Config] {
    override def initialValue: Config = null
  }

  def withExistingConf[T](conf: Config)(f: => T): T = {
    val old = existingConf.get()
    existingConf.set(conf)
    try {
      f
    } finally {
      if (old != null) {
        existingConf.set(old)
      } else {
        existingConf.remove()
      }
    }
  }

  /**
   * Defines a getter that returns the Config within scope.
   * See [[get]] for more information.
   */
  private val confGetter = new AtomicReference[() => Config](() => fallbackConf.get())

  /**
   * Sets the active config object within the current scope.
   * See [[get]] for more information.
   */
  def setConfGetter(getter: () => Config): Unit = {
    confGetter.set(getter)
  }

  /**
   * Returns the active config object within the current scope.
   */
  def get: Config = {
    val conf = existingConf.get()
    if (conf != null) {
      conf
    } else {
      confGetter.get()()
    }
  }
}
