/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.help

import org.scalatest._
import matchers._
import org.gridgain.visor._

/**
 * Unit test for 'help' command.
 */
class VisorHelpCommandSpec extends FlatSpec with ShouldMatchers {
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
    commands.license.VisorLicenseCommand
    commands.deploy.VisorDeployCommand
    commands.start.VisorStartCommand

    "General help" should "properly execute via alias" in { visor ?() }
    "General help" should "properly execute w/o alias" in { visor ?() }
    "Help for 'license' command" should "properly execute" in { visor ? "license" }
    "Help for 'start' command" should "properly execute" in { visor ? "start" }
    "Help for 'deploy' command" should "properly execute" in { visor ? "deploy" }
    "Help for 'events' command" should "properly execute" in { visor ? "events" }
    "Help for 'mclear' command" should "properly execute" in { visor ? "mclear" }
    "Help for 'cache' command" should "properly execute" in { visor ? "cache" }
    "Help for 'disco' command" should "properly execute" in { visor ? "disco" }
    "Help for 'alert' command" should "properly execute" in { visor ? "alert" }
    "Help for 'node' command" should "properly execute" in { visor ? "node" }
    "Help for 'vvm' command" should "properly execute" in { visor ? "vvm" }
    "Help for 'kill' command" should "properly execute" in { visor ? "kill" }
    "Help for 'top' command" should "properly execute" in { visor ? "top" }
    "Help for 'config' command" should "properly execute" in { visor ? "config" }
    "Help for 'ack' command" should "properly execute" in { visor ? "ack" }
    "Help for 'ping' command" should "properly execute" in { visor ? "ping" }
    "Help for 'close' command" should "properly execute" in { visor ? "close" }
    "Help for 'open' command" should "properly execute" in { visor ? "open" }
    "Help for 'status' command" should "properly execute" in { visor ? "status" }
    "Help for 'mset' command" should "properly execute" in { visor ? "mset" }
    "Help for 'mget' command" should "properly execute" in { visor ? "mget" }
    "Help for 'mlist' command" should "properly execute" in { visor ? "mlist" }
    "Help for 'help' command" should "properly execute" in { visor ? "help" }
    "Help for 'log' command" should "properly execute" in { visor ? "log" }
    "Help for 'dash' command" should "properly execute" in { visor ? "dash" }
}
