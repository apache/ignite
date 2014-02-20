// @scala.file.header

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
import matchers._
import org.gridgain.visor._
import VisorGcCommand._
import commands.top.VisorTopologyCommand._

/**
 * Unit test for 'gc' command.
 *
 * @author @java.author
 * @version @java.version
 */
class VisorGcCommandSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    behavior of "A 'gc' visor command"

    override def beforeAll() {
        visor.open("-d", false)

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
