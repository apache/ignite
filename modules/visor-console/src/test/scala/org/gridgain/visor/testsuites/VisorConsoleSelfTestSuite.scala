/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.testsuites

import org.apache.ignite.IgniteSystemProperties
import org.apache.ignite.IgniteSystemProperties._
import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner

import java.net.{InetAddress, UnknownHostException}

import org.gridgain.visor.VisorTextTableSpec
import org.gridgain.visor.commands.VisorArgListSpec
import org.gridgain.visor.commands.ack.VisorAckCommandSpec
import org.gridgain.visor.commands.alert.VisorAlertCommandSpec
import org.gridgain.visor.commands.cache.{VisorCacheClearCommandSpec, VisorCacheCommandSpec, VisorCacheCompactCommandSpec}
import org.gridgain.visor.commands.config.VisorConfigurationCommandSpec
import org.gridgain.visor.commands.cswap.VisorCacheSwapCommandSpec
import org.gridgain.visor.commands.deploy.VisorDeployCommandSpec
import org.gridgain.visor.commands.disco.VisorDiscoveryCommandSpec
import org.gridgain.visor.commands.events.VisorEventsCommandSpec
import org.gridgain.visor.commands.gc.VisorGcCommandSpec
import org.gridgain.visor.commands.help.VisorHelpCommandSpec
import org.gridgain.visor.commands.kill.VisorKillCommandSpec
import org.gridgain.visor.commands.log.VisorLogCommandSpec
import org.gridgain.visor.commands.mem.VisorMemoryCommandSpec
import org.gridgain.visor.commands.node.VisorNodeCommandSpec
import org.gridgain.visor.commands.open.VisorOpenCommandSpec
import org.gridgain.visor.commands.ping.VisorPingCommandSpec
import org.gridgain.visor.commands.start.VisorStartCommandSpec
import org.gridgain.visor.commands.tasks.VisorTasksCommandSpec
import org.gridgain.visor.commands.top.VisorTopologyCommandSpec

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class VisorConsoleSelfTestSuite extends Suites (
    new VisorTextTableSpec,
    new VisorAckCommandSpec,
    new VisorAlertCommandSpec,
    new VisorCacheCommandSpec,
    new VisorCacheClearCommandSpec,
    new VisorCacheCompactCommandSpec,
    new VisorConfigurationCommandSpec,
    new VisorCacheSwapCommandSpec,
    new VisorDeployCommandSpec,
    new VisorDiscoveryCommandSpec,
    new VisorEventsCommandSpec,
    new VisorGcCommandSpec,
    new VisorHelpCommandSpec,
    new VisorKillCommandSpec,
    new VisorLogCommandSpec,
    new VisorMemoryCommandSpec,
    new VisorNodeCommandSpec,
    new VisorOpenCommandSpec,
    new VisorPingCommandSpec,
    new VisorStartCommandSpec,
    new VisorTasksCommandSpec,
    new VisorTopologyCommandSpec,
    new VisorArgListSpec
) {
    // Mimic GridTestUtils.getNextMulticastGroup behavior because it can't be imported here
    // as it will create a circular module dependency. Use the highest address.
    try {
        val locHost = InetAddress.getLocalHost

        if (locHost != null) {
            var thirdByte: Int = locHost.getAddress()(3)

            if (thirdByte < 0)
                thirdByte += 256

            System.setProperty(GG_OVERRIDE_MCAST_GRP, "229." + thirdByte + ".255.255")
        }
    }
    catch {
        case e: UnknownHostException =>
            assert(false, "Unable to get local address.")
    }
}
