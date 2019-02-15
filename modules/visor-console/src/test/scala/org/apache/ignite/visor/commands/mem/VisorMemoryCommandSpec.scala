/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    describe("A 'mcompact' visor command") {
        it("should compact variable") {
            visor.mset("key1", "value1")
            visor.mset("key3", "value3")

            visor.mset("n0", "value0")
            visor.mset("n1", "value1")
            visor.mset("n2", "value2")
            visor.mset("n3", "value3")

            visor.mset("c1", "value1")
            visor.mset("c3", "value3")

            visor.mcompact()

            assertResult(None)(visor.mgetOpt("key0"))
            assertResult(Some("value1"))(visor.mgetOpt("key1"))
            assertResult(None)(visor.mgetOpt("key2"))
            assertResult(Some("value3"))(visor.mgetOpt("key3"))

            assertResult(Some("value0"))(visor.mgetOpt("n0"))
            assertResult(Some("value1"))(visor.mgetOpt("n1"))
            assertResult(Some("value2"))(visor.mgetOpt("n2"))
            assertResult(Some("value3"))(visor.mgetOpt("n3"))

            assertResult(Some("value1"))(visor.mgetOpt("c0"))
            assertResult(Some("value3"))(visor.mgetOpt("c1"))
            assertResult(None)(visor.mgetOpt("c3"))

            visor.mlist()
        }
    }
}
