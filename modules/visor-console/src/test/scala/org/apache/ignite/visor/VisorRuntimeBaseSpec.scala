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

package org.apache.ignite.visor

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.visor.commands.open.VisorOpenCommand._
import org.scalatest._
import VisorRuntimeBaseSpec._

/**
 * Base abstract class for unit tests requiring Visor runtime.
 */
abstract class VisorRuntimeBaseSpec(private[this] val num: Int) extends FunSpec with Matchers
    with BeforeAndAfterAll with BeforeAndAfterEach {
    assert(num >= 1)

    /**
     * Gets grid configuration.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    protected def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)

        cfg
    }

    protected def openVisor() {
        visor.open(config(VISOR_INSTANCE_NAME), "n/a")
    }

    protected def closeVisorQuiet() {
        if (visor.isConnected)
            visor.close()
    }

    /**
     * Runs before all tests.
     */
    override protected def beforeAll() {
        (1 to num).foreach((n: Int) => Ignition.start(config("node-" + n)))
    }

    /**
     * Runs after all tests.
     */
    override def afterAll() {
        (1 to num).foreach((n: Int) => Ignition.stop("node-" + n, false))
    }

    override protected def beforeEach() {
        openVisor()
    }

    override protected def afterEach() {
        closeVisorQuiet()
    }
}

/** Singleton companion object. */
object VisorRuntimeBaseSpec {
    val VISOR_INSTANCE_NAME = "visor-demo-node"
}
