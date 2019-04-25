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

package org.apache.ignite.scalar.tests

import org.apache.ignite.scalar.scalar._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.util.control.Breaks._

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarReturnableSpec extends FunSpec with ShouldMatchers {
    describe("Scalar '^^'") {
        it("should work") {
            var i = 0

            breakable {
                while (true) {
                    if (i == 0)
                        println("Only once!") ^^

                    i += 1
                }
            }

            assert(i == 0)
        }

        // Ignore exception below.
        def test() = breakable {
            while (true) {
                println("Only once!") ^^
            }
        }

        it("should also work") {
            test()
        }
    }
}
