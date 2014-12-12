/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.events

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite._
import org.gridgain.grid._
import org.gridgain.visor._
import org.gridgain.visor.commands.events.VisorEventsCommand._

/**
 * Unit test for 'events' command.
 */
class VisorEventsCommandSpec extends VisorRuntimeBaseSpec(1) {
    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setGridName(name)
        cfg.setLifeCycleEmailNotification(false)

        cfg
    }

    behavior of "A 'events' visor command"

    it should "print error message when not connected" in {
        closeVisorQuiet()

        visor.events()
    }

    it should "display all events from remote node" in {
        visor.events("-id8=@n0")
    }

    it should "display top 3 events from remote node" in {
        visor.events("-id8=@n0 -c=3")
    }

    it should "print error message with invalid count" in {
        visor.events("-id8=@n0 -c=x")
    }
}
