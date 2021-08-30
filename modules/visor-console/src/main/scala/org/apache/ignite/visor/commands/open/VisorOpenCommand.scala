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

package org.apache.ignite.visor.commands.open

import java.net.URL
import java.util.logging.{ConsoleHandler, Level, Logger}

import org.apache.ignite.IgniteSystemProperties._
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgniteComponentType._
import org.apache.ignite.internal.IgniteEx
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.spring.IgniteSpringHelper
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.logger.NullLogger
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._
import org.apache.ignite.visor.{VisorTag, visor}
import org.apache.ignite.{IgniteException, IgniteSystemProperties, Ignition}

import scala.language.{implicitConversions, reflectiveCalls}

/**
 * ==Overview==
 * Contains Visor command `node` implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | node | Prints node statistics. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     node "{-id8=<node-id8>|-id=<node-id>} {-a}"
 *     node
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8=<node-id8>
 *         ID8 of node. Either '-id8' or '-id' can be specified.
 *         If neither specified - command starts in interactive mode.
 *     -id=<node-id>
 *         Full ID of node. Either '-id8' or '-id' can  be specified.
 *         If neither specified - command starts in interactive mode.
 *     -a
 *         Print extended information.
 *         By default - only abbreviated statistics is printed.
 * }}}
 *
 * ====Examples====
 * {{{
 *     node
 *         Starts command in interactive mode.
 *     node "-id8=12345678"
 *         Prints statistics for specified node.
 *     node "-id8=12345678 -a"
 *         Prints full statistics for specified node.
 * }}}
 */
class VisorOpenCommand extends VisorConsoleCommand {
    @impl protected val name = "open"

    /** Default configuration path relative to Ignite home. */
    private final val DFLT_CFG = "config/default-config.xml"

    /**
     * ==Command==
     * Connects Visor console to the default grid.
     *
     * ==Example==
     * <ex>open</ex>
     * Connects to the default grid.
     */
    def open() {
        open("")
    }

    /**
     * ==Command==
     * Connects Visor console to default or named grid.
     *
     * ==Examples==
     * <ex>open -g=mygrid</ex>
     * Connects to 'mygrid' grid.
     *
     * @param args Command arguments.
     */
    def open(args: String) {
        assert(args != null)

        if (isConnected) {
            warn("Visor is already connected. Disconnect first.")

            return
        }

        try {
            def configuration(path: String): IgniteConfiguration = {
                assert(path != null)

                val url =
                    try
                        new URL(path)
                    catch {
                        case e: Exception =>
                            val url = U.resolveIgniteUrl(path)

                            if (url == null)
                                throw new IgniteException("Ignite configuration path is invalid: " + path, e)

                            url
                    }

                // Add no-op logger to remove no-appender warning.
                val log4jTup =
                    if (visor.quiet) {
                        val springLog = Logger.getLogger("org.springframework")

                        if (springLog != null)
                            springLog.setLevel(Level.WARNING)

                        null
                    }
                    else if (classOf[Ignition].getClassLoader.getResource("org/apache/log4j/Appender.class") != null)
                        U.addLog4jNoOpLogger()
                    else
                        null

                val spring: IgniteSpringHelper = SPRING.create(false)

                val cfgs =
                    try
                        // Cache, indexing SPI configurations should be excluded from daemon node config.
                        spring.loadConfigurations(url, "cacheConfiguration", "lifecycleBeans", "indexingSpi").get1()
                    finally {
                        if (log4jTup != null && !visor.quiet)
                            U.removeLog4jNoOpLogger(log4jTup)
                    }

                if (cfgs == null || cfgs.isEmpty)
                    throw new IgniteException("Can't find grid configuration in: " + url)

                if (cfgs.size > 1)
                    throw new IgniteException("More than one grid configuration found in: " + url)

                val cfg = cfgs.iterator().next()

                if (visor.quiet)
                    cfg.setGridLogger(new NullLogger)
                else {
                    if (log4jTup != null)
                        System.setProperty(IgniteSystemProperties.IGNITE_CONSOLE_APPENDER, "false")
                    else
                        Logger.getGlobal.getHandlers.foreach({
                            case handler: ConsoleHandler => Logger.getGlobal.removeHandler(handler)
                        })
                }

                // Setting up 'Config URL' for properly print in console.
                System.setProperty(IgniteSystemProperties.IGNITE_CONFIG_URL, url.getPath)

                cfg.setConnectorConfiguration(null)

                var ioSpi = cfg.getCommunicationSpi

                if (ioSpi == null)
                    ioSpi = new TcpCommunicationSpi()

                cfg
            }

            val argLst = parseArgs(args)

            val path = argValue("cpath", argLst)
            val dflt = hasArgFlag("d", argLst)

            val (cfg, cfgPath) =
                if (path.isDefined)
                    (configuration(path.get), path.get)
                else if (dflt)
                    (configuration(DFLT_CFG), "<default>")
                else {
                    // If configuration file is not defined in arguments,
                    // ask to choose from the list
                    askConfigFile() match {
                        case Some(p) =>
                            nl()

                            (VisorTextTable() +=("Using configuration", p)) render()

                            nl()

                            (configuration(p), p)
                        case None =>
                            return
                    }
                }

            open(cfg, cfgPath)
        }
        catch {
            case e: IgniteException =>
                warn("Type 'help open' to see how to use this command.")

                status("q")

                throw e;
        }
    }

    /**
     * Connects Visor console to configuration with path.
     *
     * @param cfg Configuration.
     * @param cfgPath Configuration path.
     */
    def open(cfg: IgniteConfiguration, cfgPath: String) = {
        val daemon = Ignition.isDaemon

        val shutdownHook = IgniteSystemProperties.getString(IGNITE_NO_SHUTDOWN_HOOK, "false")

        // Make sure Visor console starts as daemon node.
        Ignition.setDaemon(true)

        // Make sure visor starts without shutdown hook.
        System.setProperty(IGNITE_NO_SHUTDOWN_HOOK, "true")

        ignite = try {
            // We need to stop previous daemon node before to start new one.
            prevIgnite.foreach(g => Ignition.stop(g.name(), true))

            Ignition.start(cfg).asInstanceOf[IgniteEx]
        }
        finally {
            Ignition.setDaemon(daemon)

            System.setProperty(IGNITE_NO_SHUTDOWN_HOOK, shutdownHook)
        }

        prevIgnite = Some(ignite)

        visor.open(ignite.name(), cfgPath)
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorOpenCommand {
    /** Singleton command. */
    private val cmd = new VisorOpenCommand

    // Adds command's help to visor.
    addHelp(
        name = "open",
        shortInfo = "Connects Visor console to the grid.",
        longInfo = Seq(
            "Connects Visor console to the grid. Note that P2P class loading",
            "should be enabled on all nodes.",
            " ",
            "If neither '-cpath' or '-d' are provided, command will ask",
            "user to select Ignite configuration file in interactive mode."
        ),
        spec = Seq(
            "open -cpath=<path>",
            "open -d"
        ),
        args = Seq(
            "-cpath=<path>" -> Seq(
                "Ignite configuration path.",
                "Can be absolute, relative to Ignite home folder or any well formed URL."
            ),
            "-d" -> Seq(
                "Flag forces the command to connect to grid using default Ignite configuration file.",
                "without interactive mode."
            )
        ),
        examples = Seq(
            "open" ->
                "Prompts user to select Ignite configuration file in interactive mode.",
            "open -d" ->
                "Connects Visor console to grid using default Ignite configuration file.",
            "open -cpath=/gg/config/mycfg.xml" ->
                "Connects Visor console to grid using Ignite configuration from provided file."
        ),
        emptyArgs = cmd.open,
        withArgs = cmd.open
    )

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromNode2Visor(vs: VisorTag): VisorOpenCommand = cmd
}
