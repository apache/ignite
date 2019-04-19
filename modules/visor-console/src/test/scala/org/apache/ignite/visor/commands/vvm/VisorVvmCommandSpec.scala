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

package org.apache.ignite.visor.commands.vvm

import org.apache.ignite.visor.visor
import org.scalatest._

import org.apache.ignite.visor.commands.open.VisorOpenCommand._
import org.apache.ignite.visor.commands.vvm.VisorVvmCommand._

/**
 * Unit test for 'vvm' command.
 */
class VisorVvmCommandSpec extends FunSpec with Matchers {
    describe("A 'vvm' visor command") {
        it("should print error message when not connected") {
            visor.vvm()
        }

        it("should open VisualVM connected to all nodes skipping ones with disabled JMX") {
            visor.open("-d")
            visor.vvm()
            visor.close()
        }

        it("should open VisualVM connected to first node if it has JMX enabled") {
            visor.open("-d")
            visor.vvm("-id8=@n1")
            visor.close()
        }
    }
}
