/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */
package org.gridgain.visor

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.scalatest._

/**
 * Base abstract class for unit tests requiring Visor runtime.
 */
abstract class VisorRuntimeBaseSpec(private[this] val num: Int) extends FlatSpec with Matchers
    with BeforeAndAfterAll with BeforeAndAfterEach {
    assert(num >= 1)

    /**
     * Gets grid configuration.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    protected def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setGridName(name)

        cfg
    }

    protected def openVisor() {
        visor.open(config("visor-demo-node"), "n/a")
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
