/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.open

import org.gridgain.visor._

/**
 * Unit test for 'open' command.
 */
class VisorOpenCommandSpec extends VisorRuntimeBaseSpec(3) {
    behavior of "A 'open' visor command"

    it should "properly connect using default configuration" in {
        visor open("-d", false)
        visor mlist()
        visor close()
    }

    it should "print error message when already connected" in {
        visor open("-d", false)
        visor open("-d", false)
        visor close()
    }

    it should "print error message about wrong parameter combination" in {
        visor open("-d -e", false)
    }
}
