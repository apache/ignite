/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.config

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.events.IgniteEventType._

import org.gridgain.visor._
import org.gridgain.visor.commands.config.VisorConfigurationCommand._

/**
 * Unit test for 'config' command.
 */
class VisorConfigurationCommandSpec extends VisorRuntimeBaseSpec(1) {
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
        cfg.setIncludeEventTypes(EVTS_ALL: _*)

        cfg
    }

    behavior of "A 'config' visor command"

    it should "print configuration for first node" in {
        visor.config("-id8=@n0")
    }
}
