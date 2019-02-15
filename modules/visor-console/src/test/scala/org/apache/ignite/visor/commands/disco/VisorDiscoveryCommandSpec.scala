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

package org.apache.ignite.visor.commands.disco

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}

import org.apache.ignite.visor.commands.disco.VisorDiscoveryCommand._

/**
 * Unit test for 'disco' command.
 */
class VisorDiscoveryCommandSpec extends VisorRuntimeBaseSpec(4) {
    /**
     * Open visor and execute several tasks before all tests.
     */
    override protected def beforeAll() {
        super.beforeAll()

        Ignition.stop("node-1", false)
        Ignition.stop("node-2", false)
    }

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)

        cfg
    }

    describe("A 'disco' visor command") {
        it("should advise to connect") {
            closeVisorQuiet()

            visor.disco()
        }

        it("should show all discovery events") {
            visor.disco()
        }

        it("should show all discovery events in reversed order") {
            visor.disco("-r")
        }

        it("should show discovery events from last two minutes") {
            visor.disco("-t=2m")
        }

        it("should show discovery events from last two minutes in reversed order ") {
            visor.disco("-t=2m -r")
        }

        it("should show top 3 discovery events") {
            visor.disco("-c=3")
        }

        it("should print error message with invalid count") {
            visor.disco("-c=x")
        }
    }
}
