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

package org.apache.ignite.visor.commands

import org.apache.ignite.visor.visor
import org.scalatest._

import visor._

/**
 * Test for visor's argument list parsing.
 */
class VisorArgListSpec extends FunSpec with ShouldMatchers {
    describe("A visor argument list") {
        it("should properly parse 'null' arguments") {
            val v = parseArgs(null)

            assert(v.isEmpty)
        }

        it("should properly parse non-empty arguments") {
            val v = parseArgs("-a=b c d -minus -d=")

            assert(v.size == 5)

            assert(v(0)._1 == "a")
            assert(v(0)._2 == "b")

            assert(v(1)._1 == null)
            assert(v(1)._2 == "c")

            assert(v(2)._1 == null)
            assert(v(2)._2 == "d")

            assert(v(3)._1 == "minus")
            assert(v(3)._2 == null)

            assert(v(4)._1 == "d")
            assert(v(4)._2 == "")
        }

        it("should properly parse quoted arguments") {
            val v = parseArgs("-a='b 'c' d' -minus -d=")

            assert(v.size == 3)

            assert(v(0)._1 == "a")
            assert(v(0)._2 == "b 'c' d")

            assert(v(1)._1 == "minus")
            assert(v(1)._2 == null)

            assert(v(2)._1 == "d")
            assert(v(2)._2 == "")
        }
    }
}
