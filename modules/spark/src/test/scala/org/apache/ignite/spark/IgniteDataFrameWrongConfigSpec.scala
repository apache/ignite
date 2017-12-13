/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark

import java.lang.{Integer ⇒ JInteger, String ⇒ JString}

import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameOptions._
import org.apache.ignite.{IgniteException, IgniteIllegalStateException}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Negative tests to check errors in case of wrong configuration.
  */
@RunWith(classOf[JUnitRunner])
class IgniteDataFrameWrongConfigSpec extends AbstractDataFrameSpec {
    describe("DataFrame negative cases") {
        it("Should throw exception when try load unknown grid") {
            intercept[IgniteIllegalStateException] {
                spark.read
                    .format(IGNITE)
                    .option(GRID, "unknown_grid")
                    .option(CACHE, "cache1")
                    .option(KEY_CLASS, classOf[JInteger].getName)
                    .option(VALUE_CLASS, classOf[JString].getName)
                    .load()
            }
        }

        it("Should throw exception when try load unknown table") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(TABLE, "unknown_table")
                    .load()
            }
        }

        it("Should throw exception when try load unknown cache") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(CACHE, "unknown_cache")
                    .option(KEY_CLASS, classOf[JInteger].getName)
                    .option(VALUE_CLASS, classOf[JString].getName)
                    .load()
            }
        }

        it("Should throw exception when no cache and no table") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .load()
            }
        }

        it("Should throw exception when no key class for cache") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(CACHE, "cache1")
                    .option(VALUE_CLASS, classOf[JString].getName)
                    .load()
            }
        }

        it("Should throw exception when no value class for cache") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(CACHE, "cache1")
                    .option(KEY_CLASS, classOf[JString].getName)
                    .load()
            }
        }

        it("Should throw exception when no class for cache") {
            intercept[IgniteException] {
                spark.read
                    .format(IGNITE)
                    .option(CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(CACHE, "cache1")
                    .load()
            }
        }
    }
}

