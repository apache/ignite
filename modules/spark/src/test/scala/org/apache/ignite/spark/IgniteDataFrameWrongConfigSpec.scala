/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark

import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.IgniteException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Negative tests to check errors in case of wrong configuration.
  */
@RunWith(classOf[JUnitRunner])
class IgniteDataFrameWrongConfigSpec extends AbstractDataFrameSpec {
    describe("DataFrame negative cases") {
        it("Should throw exception when try load unknown table") {
            intercept[IgniteException] {
                spark.read
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, "unknown_table")
                    .load()
            }
        }

        it("Should throw exception when no cache and no table") {
            intercept[IgniteException] {
                spark.read
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .load()
            }
        }
    }
}
