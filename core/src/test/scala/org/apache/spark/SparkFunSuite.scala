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

import org.apache.commons.io.FileUtils

import org.apache.spark.deploy.LocalSparkCluster
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.util.AccumulatorContext

abstract class SparkFunSuite extends BaseSparkFunSuite {
  protected override def beforeAll(): Unit = {
    System.setProperty(IS_TESTING.key, "true")
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      // Avoid leaking map entries in tests that use accumulators without SparkContext
      AccumulatorContext.clear()
    } finally {
      super.afterAll()
    }
  }

  protected override def logForFailedTest(): Unit = {
    LocalSparkCluster.get.foreach { localCluster =>
      val workerLogfiles = localCluster.workerLogfiles
      if (workerLogfiles.nonEmpty) {
        logInfo("\n\n===== EXTRA LOGS FOR THE FAILED TEST\n")
        workerLogfiles.foreach { logFile =>
          logInfo(s"\n----- Logfile: ${logFile.getAbsolutePath()}")
          logInfo(FileUtils.readFileToString(logFile, "UTF-8"))
        }
      }
    }
  }
}
