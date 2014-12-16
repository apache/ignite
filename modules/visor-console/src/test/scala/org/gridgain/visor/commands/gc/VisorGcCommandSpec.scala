/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.gc

import org.scalatest._

import org.gridgain.visor._
import org.gridgain.visor.commands.gc.VisorGcCommand._
import org.gridgain.visor.commands.top.VisorTopologyCommand._

/**
 * Unit test for 'gc' command.
 */
class VisorGcCommandSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    behavior of "A 'gc' visor command"

    override def beforeAll() {
        visor.open("-d")

        visor.top()
    }

    override def afterAll() {
        visor.close()
    }

    it should "run GC on all nodes" in {
        visor.gc()
    }

    it should "run GC on first node" in {
        visor.gc("-id8=@n0")
    }

    it should "run GC and DGC on all nodes" in {
        visor.gc("-c")
    }

    it should "run GC and DGC on first node" in {
        visor.gc("-id8=@n0 -c")
    }
}
