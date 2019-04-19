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

package org.apache.ignite.visor.commands.gc

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.visor.commands.gc.VisorGcCommand._
import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}

/**
 * Unit test for 'gc' command.
 */
class VisorGcCommandSpec extends VisorRuntimeBaseSpec(1) {
    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration =
    {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)

        cfg
    }

    describe("'gc' visor command") {
        it("should run GC on all nodes") {
            visor.gc()
        }

        it("should run GC on first node") {
            visor.gc("-id8=@n0")
        }

        it("should run GC and DGC on all nodes") {
            visor.gc("-c")
        }

        it("should run GC and DGC on first node") {
            visor.gc("-id8=@n0 -c")
        }

    }
}
