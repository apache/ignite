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

package org.apache.ignite.visor.commands.help

import org.apache.ignite.visor._
import org.scalatest._

/**
 * Unit test for 'help' command.
 */
class VisorHelpCommandSpec extends FunSpec with Matchers {
    // Pre-initialize command so that help can be registered.
    commands.ack.VisorAckCommand
    commands.ping.VisorPingCommand
    commands.alert.VisorAlertCommand
    commands.config.VisorConfigurationCommand
    commands.top.VisorTopologyCommand
    commands.kill.VisorKillCommand
    commands.vvm.VisorVvmCommand
    commands.node.VisorNodeCommand
    commands.events.VisorEventsCommand
    commands.disco.VisorDiscoveryCommand
    commands.cache.VisorCacheCommand
    commands.start.VisorStartCommand
    commands.deploy.VisorDeployCommand
    commands.start.VisorStartCommand


    describe("General help") {
        it ("should properly execute via alias") { visor.searchCmd("?").get.emptyArgs }
        it ("should properly execute w/o alias") { visor.searchCmd("help").get.emptyArgs }
    }

    describe("Help for command") {
        it ("should properly execute for 'start' command") { visor.help("start") }
        it ("should properly execute for 'deploy' command") { visor.help("deploy") }
        it ("should properly execute for 'events' command") { visor.help("events") }
        it ("should properly execute for 'mclear' command") { visor.help("mclear") }
        it ("should properly execute for 'cache' command") { visor.help("cache") }
        it ("should properly execute for 'disco' command") { visor.help("disco") }
        it ("should properly execute for 'alert' command") { visor.help("alert") }
        it ("should properly execute for 'node' command") { visor.help("node") }
        it ("should properly execute for 'vvm' command") { visor.help("vvm") }
        it ("should properly execute for 'kill' command") { visor.help("kill") }
        it ("should properly execute for 'top' command") { visor.help("top") }
        it ("should properly execute for 'config' command") { visor.help("config") }
        it ("should properly execute for 'ack' command") { visor.help("ack") }
        it ("should properly execute for 'ping' command") { visor.help("ping") }
        it ("should properly execute for 'close' command") { visor.help("close") }
        it ("should properly execute for 'open' command") { visor.help("open") }
        it ("should properly execute for 'start' status") { visor.help("status") }
        it ("should properly execute for 'start' mset") { visor.help("mset") }
        it ("should properly execute for 'start' mget") { visor.help("mget") }
        it ("should properly execute for 'start' mlist") { visor.help("mlist") }
        it ("should properly execute for 'start' log") { visor.help("log") }
        it ("should properly execute for 'start' dash") { visor.help("dash") }
    }
}
