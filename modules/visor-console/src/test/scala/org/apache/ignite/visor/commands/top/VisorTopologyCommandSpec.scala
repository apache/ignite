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
