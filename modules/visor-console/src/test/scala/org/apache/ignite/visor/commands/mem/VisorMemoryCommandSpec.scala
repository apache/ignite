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

package org.apache.ignite.visor.commands.mem

import org.apache.ignite.visor.visor
import org.scalatest._

/**
 * Unit test for memory commands.
 */
class VisorMemoryCommandSpec extends FunSpec with Matchers {
    describe("A 'mget' visor command") {
        it("should get correct value") {
            visor.mset("key", "value")

            assertResult(Option("value"))(visor.mgetOpt("key"))

            visor.mclear()
        }
    }

    describe("A 'mlist' visor command") {
        it("should list all variables") {
            visor.mset("key1", "value1")
            visor.mset("key2", "value2")
            visor.mset("key3", "value3")

            visor.mlist()
            visor.mclear()
        }

        it("should list ax and cx variables") {
            visor.mset("a1", "1")
            visor.mset("a2", "2")
            visor.mset("b1", "3")
            visor.mset("b2", "4")
            visor.mset("c1", "5")
            visor.mset("c2", "6")

            visor.mlist("ac")
            visor.mclear()
        }
    }

    describe("A 'mclear' visor command") {
        it("should remove first two variables") {
            visor.mset("key1", "value1")
            visor.mset("key2", "value2")
            visor.mset("key3", "value3")

            visor mclear "key1 key2"

            visor.mlist()
            visor.mclear()

        }

        it("should remove all variables") {
            visor.mset("key1", "value1")
            visor.mset("key2", "value2")
            visor.mset("key3", "value3")

            visor.mclear()
            visor.mlist()
        }
    }
}
