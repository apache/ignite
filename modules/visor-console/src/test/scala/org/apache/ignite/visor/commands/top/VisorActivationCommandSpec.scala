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

import org.apache.ignite.Ignition
import org.apache.ignite.configuration._
import org.apache.ignite.visor.commands.top.VisorTopologyCommand._
import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}
import VisorRuntimeBaseSpec._

/**
 * Unit test for cluster activation commands.
 */
class VisorActivationCommandSpec extends VisorRuntimeBaseSpec(2) {
    override protected def config(name: String): IgniteConfiguration = {
        val cfg = super.config(name)

        val dfltReg = new DataRegionConfiguration
        val dataRegCfg = new DataStorageConfiguration

        dfltReg.setMaxSize(10 * 1024 * 1024)
        dfltReg.setPersistenceEnabled(true)
        dataRegCfg.setDefaultDataRegionConfiguration(dfltReg)

        cfg.setDataStorageConfiguration(dataRegCfg)

        cfg
    }

    describe("A 'top' visor command for cluster activation") {
        it("should activate cluster") {
            assert(!Ignition.ignite(VISOR_INSTANCE_NAME).active())

            visor.top()

            visor.top("-activate")

            visor.top()

            assert(Ignition.ignite(VISOR_INSTANCE_NAME).active())
        }

        it("should deactivate cluster") {
            assert(Ignition.ignite(VISOR_INSTANCE_NAME).active())

            visor.top()

            visor.top("-deactivate")

            visor.top()

            assert(!Ignition.ignite(VISOR_INSTANCE_NAME).active())
        }
    }
}
