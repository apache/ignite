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

import org.gridgain.visor._

/**
 * Unit test for 'help' command.
 */
class VisorHelpCommandSpec extends FlatSpec with Matchers {
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

    "General help" should "properly execute via alias" in { visor.help() }
    "General help" should "properly execute w/o alias" in { visor.help() }
    "Help for 'start' command" should "properly execute" in { visor.help("start") }
    "Help for 'deploy' command" should "properly execute" in { visor.help("deploy") }
    "Help for 'events' command" should "properly execute" in { visor.help("events") }
    "Help for 'mclear' command" should "properly execute" in { visor.help("mclear") }
    "Help for 'cache' command" should "properly execute" in { visor.help("cache") }
    "Help for 'disco' command" should "properly execute" in { visor.help("disco") }
    "Help for 'alert' command" should "properly execute" in { visor.help("alert") }
    "Help for 'node' command" should "properly execute" in { visor.help("node") }
    "Help for 'vvm' command" should "properly execute" in { visor.help("vvm") }
    "Help for 'kill' command" should "properly execute" in { visor.help("kill") }
    "Help for 'top' command" should "properly execute" in { visor.help("top") }
    "Help for 'config' command" should "properly execute" in { visor.help("config") }
    "Help for 'ack' command" should "properly execute" in { visor.help("ack") }
    "Help for 'ping' command" should "properly execute" in { visor.help("ping") }
    "Help for 'close' command" should "properly execute" in { visor.help("close") }
    "Help for 'open' command" should "properly execute" in { visor.help("open") }
    "Help for 'status' command" should "properly execute" in { visor.help("status") }
    "Help for 'mset' command" should "properly execute" in { visor.help("mset") }
    "Help for 'mget' command" should "properly execute" in { visor.help("mget") }
    "Help for 'mlist' command" should "properly execute" in { visor.help("mlist") }
    "Help for 'help' command" should "properly execute" in { visor.help("help") }
    "Help for 'log' command" should "properly execute" in { visor.help("log") }
    "Help for 'dash' command" should "properly execute" in { visor.help("dash") }
}
