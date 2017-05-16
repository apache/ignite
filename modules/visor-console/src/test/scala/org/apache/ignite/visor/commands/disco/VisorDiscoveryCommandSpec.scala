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
