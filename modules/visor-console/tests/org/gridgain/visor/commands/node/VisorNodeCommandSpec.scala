/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.node

import org.gridgain.visor._
import VisorNodeCommand._

/**
 * Unit test for 'node' command.
 */
class VisorNodeCommandSpec extends VisorRuntimeBaseSpec(1) {
    behavior of "A 'node' visor command"

    it should "properly execute with valid node ID" in {
        visor open("-d", false)
        visor node("-id8=@n1")
        visor close()
    }

    it should "print the error message for invalid node ID" in {
        visor open("-d", false)
        visor node("-id8=zeee")
        visor close()
    }

    it should "print error message when not connected" in {
        visor node("") // Arguments are ignored.
    }
}
