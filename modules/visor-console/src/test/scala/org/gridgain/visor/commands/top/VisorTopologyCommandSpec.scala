/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.top

import org.gridgain.visor._
import org.gridgain.visor.commands.top.VisorTopologyCommand._

/**
 * Unit test for topology commands.
 */
class VisorTopologyCommandSpec extends VisorRuntimeBaseSpec(2) {
    behavior of "A 'top' visor command"

    it should "advise to connect" in {
        closeVisorQuiet()

        visor.top()
    }

    it should "print error message" in {
        visor.top("-cc=eq1x")
    }

    it should "print full topology" in {
        visor.top()
    }

    it should "print nodes with idle time greater than 12000ms" in {
        visor.top("-it=gt12000")
    }

    it should "print nodes with idle time greater than 12sec" in {
        visor.top("-it=gt12s")
    }

    it should "print full information about all nodes" in {
        visor.top("-a")
    }

    it should "print information about nodes on localhost" in {
        visor.top("-h=192.168.1.100")
    }

    it should "print full information about nodes on localhost" in {
        visor.top("-h=localhost")
    }
}
