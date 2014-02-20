// @scala.file.header

/*
* ___    _________________________ ________
* __ |  / /____  _/__  ___/__  __ \___  __ \
* __ | / /  __  /  _____ \ _  / / /__  /_/ /
* __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
* _____/   /___/   /____/  \____/  /_/ |_|
*
*/

package org.gridgain.visor.commands.log

import org.scalatest._
import matchers._
import org.gridgain.visor._

/**
* Unit test for 'log' command.
*
* @author @java.author
* @version @java.version
*/
class VisorLogCommandSpec extends FlatSpec with ShouldMatchers {
    behavior of "A 'log' visor command"

    it should "print log status" in {
        visor.log()
    }
}
