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

package org.apache.ignite.visor.commands.top

import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}
import org.apache.ignite.visor.commands.top.VisorTopologyCommand._

/**
 * Unit test for topology commands.
 */
class VisorTopologyCommandSpec extends VisorRuntimeBaseSpec(2) {
    describe("A 'top' visor command") {
        it("should advise to connect") {
            closeVisorQuiet()

            visor.top()
        }

        it("should print error message") {
            visor.top("-cc=eq1x")
        }

        it("should print full topology") {
            visor.top()
        }

        it("should print nodes with idle time greater than 12000ms") {
            visor.top("-it=gt12000")
        }

        it("should print nodes with idle time greater than 12sec") {
            visor.top("-it=gt12s")
        }

        it("should print full information about all nodes") {
            visor.top("-a")
        }

        it("should print information about nodes on localhost") {
            visor.top("-h=192.168.1.100")
        }

        it("should print full information about nodes on localhost") {
            visor.top("-h=localhost")
        }
    }
}
