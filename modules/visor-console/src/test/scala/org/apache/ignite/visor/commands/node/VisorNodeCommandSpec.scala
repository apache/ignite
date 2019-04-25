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

package org.apache.ignite.visor.commands.node

import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}
import org.apache.ignite.visor.commands.node.VisorNodeCommand._

/**
 * Unit test for 'node' command.
 */
class VisorNodeCommandSpec extends VisorRuntimeBaseSpec(1) {
    describe("A 'node' visor command") {
        it("should properly execute with valid node ID") {
            visor.node("-id8=@n1")
        }

        it("should print the error message for invalid node ID") {
            visor.node("-id8=zeee")
        }

        it("should print error message when not connected") {
            closeVisorQuiet()

            visor.node("") // Arguments are ignored.
        }
    }
}
