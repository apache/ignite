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

package org.gridgain.visor.commands.mem

import org.scalatest._

import org.gridgain.visor._

/**
 * Unit test for memory commands.
 */
class VisorMemoryCommandSpec extends FlatSpec with Matchers {
    "A 'mget' visor command" should "get correct value" in {
        visor.mset("key", "value")

        assertResult(Option("value"))(visor.mgetOpt("key"))

        visor.mclear()
    }

    "A 'mlist' visor command" should "list all variables" in {
        visor.mset("key1", "value1")
        visor.mset("key2", "value2")
        visor.mset("key3", "value3")

        visor.mlist()
        visor.mclear()
    }

    "A 'mlist' visor command" should "list ax and cx variables" in {
        visor.mset("a1", "1")
        visor.mset("a2", "2")
        visor.mset("b1", "3")
        visor.mset("b2", "4")
        visor.mset("c1", "5")
        visor.mset("c2", "6")

        visor.mlist("ac")
        visor.mclear()
    }

    "A 'mclear' visor command" should "remove first two variables" in {
        visor.mset("key1", "value1")
        visor.mset("key2", "value2")
        visor.mset("key3", "value3")

        visor mclear "key1 key2"

        visor.mlist()
        visor.mclear()
    }

    "A 'mclear' visor command" should "remove all variables" in {
        visor.mset("key1", "value1")
        visor.mset("key2", "value2")
        visor.mset("key3", "value3")

        visor.mclear()
        visor.mlist()
    }
}
