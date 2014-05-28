/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.disco

import org.gridgain.grid.{GridGain => G, _}

import org.scalatest._

import org.gridgain.visor._
import org.gridgain.visor.commands.disco.VisorDiscoveryCommand._

/**
 * Unit test for 'disco' command.
 */
class VisorDiscoveryCommandSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    /**
     * Open visor and execute several tasks before all tests.
     */
    override def beforeAll() {
        G.start(config("grid-1"))
        G.start(config("grid-2"))
        G.start(config("grid-3"))
        G.start(config("grid-4"))

        G.stop("grid-1", false)
        G.stop("grid-2", false)
    }

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    private def config(name: String): GridConfiguration = {
        val cfg = new GridConfiguration

        cfg.setGridName(name)
        cfg.setLifeCycleEmailNotification(false)

        cfg
    }

    /**
     * Close visor after all tests.
     */
    override def afterAll() {
        visor.close()

        G.stopAll(false)
    }

    behavior of  "A 'disco' visor command"

    it should "advise to connect" in  {
        visor.disco()
    }

    it should "show all discovery events" in  {
        visor.open("-d", false)
        visor.disco()
        visor.close()
    }

    it should "show all discovery events in reversed order" in  {
        visor.open("-d", false)
        visor.disco("-r")
        visor.close()
    }

    it should "show discovery events from last two minutes" in {
        visor.open("-d", false)
        visor.disco("-t=2m")
        visor.close()
    }

    it should "show discovery events from last two minutes in reversed order " in {
        visor.open("-d", false)
        visor.disco("-t=2m -r")
        visor.close()
    }

    it should "show top 3 discovery events" in  {
        visor.open("-d", false)
        visor.disco("-c=3")
        visor.close()
    }

    it should "print error message with invalid count" in {
        visor.open("-d", false)
        visor.disco("-c=x")
        visor.close()
    }
}
