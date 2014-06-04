/* @scala.file.header */

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
import org.gridgain.visor.commands.ack.VisorAckCommand._

/**
 * Unit test for 'ack' command.
 */
class VisorAckCommandSpec extends VisorRuntimeBaseSpec(2) {
    behavior of "A 'ack' visor command"

    it should "properly execute w/o arguments" in {
        visor ack()
    }

    it should "properly execute with arguments" in {
        visor ack "Broadcasting!"
    }

    it should "print error message when not connected" in {
        visor ack()
    }
}
