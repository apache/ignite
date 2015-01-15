/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
