/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.visor.testsuites

import java.net.{InetAddress, UnknownHostException}

import org.apache.ignite.IgniteSystemProperties._
import org.apache.ignite.visor.VisorTextTableSpec
import org.apache.ignite.visor.commands.VisorArgListSpec
import org.apache.ignite.visor.commands.ack.VisorAckCommandSpec
import org.apache.ignite.visor.commands.alert.VisorAlertCommandSpec
import org.apache.ignite.visor.commands.cache.{VisorCacheClearCommandSpec, VisorCacheCommandSpec}
import org.apache.ignite.visor.commands.config.VisorConfigurationCommandSpec
import org.apache.ignite.visor.commands.deploy.VisorDeployCommandSpec
import org.apache.ignite.visor.commands.disco.VisorDiscoveryCommandSpec
import org.apache.ignite.visor.commands.events.VisorEventsCommandSpec
import org.apache.ignite.visor.commands.gc.VisorGcCommandSpec
import org.apache.ignite.visor.commands.help.VisorHelpCommandSpec
import org.apache.ignite.visor.commands.kill.VisorKillCommandSpec
import org.apache.ignite.visor.commands.log.VisorLogCommandSpec
import org.apache.ignite.visor.commands.mem.VisorMemoryCommandSpec
import org.apache.ignite.visor.commands.node.VisorNodeCommandSpec
import org.apache.ignite.visor.commands.open.VisorOpenCommandSpec
import org.apache.ignite.visor.commands.ping.VisorPingCommandSpec
import org.apache.ignite.visor.commands.start.VisorStartCommandSpec
import org.apache.ignite.visor.commands.tasks.VisorTasksCommandSpec
import org.apache.ignite.visor.commands.top.{VisorActivationCommandSpec, VisorTopologyCommandSpec}
import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner

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
    new VisorConfigurationCommandSpec,
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
    new VisorActivationCommandSpec,
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

            System.setProperty(IGNITE_OVERRIDE_MCAST_GRP, "229." + thirdByte + ".255.255")
        }
    }
    catch {
        case e: UnknownHostException =>
            assert(false, "Unable to get local address.")
    }
}
