// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.ack

import org.gridgain.visor._
import VisorAckCommand._

/**
 * Unit test for 'ack' command.
 *
 * @author @java.author
 * @version @java.version
 */
class VisorAckCommandSpec extends VisorRuntimeBaseSpec(2) {
    behavior of "A 'ack' visor command"

    it should "properly execute w/o arguments" in {
        visor open("-d", false)
        visor ack()
        visor close()
    }

    it should "properly execute with arguments" in {
        visor open("-d", false)
        visor ack("Broadcasting!")
        visor close()
    }

    it should "print error message when not connected" in {
        visor ack()
    }
}
