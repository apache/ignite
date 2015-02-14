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

package org.apache.ignite.visor.commands.config

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.internal.visor.node.VisorNodeConfigurationCollectorTask
import org.apache.ignite.lang.IgniteBiTuple

import java.lang.System._

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'config' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.apache.ignite.visor._
 * import commands.config.VisorConfigurationCommand._
 * </ex>
 * Note that `VisorConfigurationCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +-------------------------------------+
 * | config | Prints node configuration. |
 * +-------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     config
 *     config "{-id=<node-id>|id8=<node-id8>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full node ID. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 *     -id8=<node-id8>
 *         Node ID8. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 * }}}
 *
 * ====Examples====
 * {{{
 *     config "-id8=12345678"
 *         Prints configuration for node with '12345678' ID8.
 *     config
 *         Starts command in interactive mode.
 * }}}
 */
class VisorConfigurationCommand {
    /** Default value */
    private val DFLT = "<n/a>"

    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help config' to see how to use this command.")
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    private def arr2Str[T: ClassTag](arr: Array[T]): String = {
        if (arr != null && arr.length > 0) U.compact(arr.mkString(", ")) else DFLT
    }

    /**
     * Converts `Boolean` to 'Yes'/'No' string.
     *
     * @param bool Boolean value.
     * @return String.
     */
    private def bool2Str(bool: Boolean): String = {
        if (bool) "on" else "off"
    }

    /**
      * ===Command===
      * Run command in interactive mode.
      *
      * ===Examples===
      * <ex>config</ex>
      * Starts command in interactive mode.
     */
    def config() {
        if (!isConnected)
            adviseToConnect()
        else
            askForNode("Select node from:") match {
                case Some(id) => config("-id=" + id)
                case None => ()
            }
    }

    /**
     * ===Command===
     * Prints configuration of specified node including all caches.
     *
     * ===Examples===
     * <ex>config "-id8=12345678"</ex>
     * Prints configuration of node with '12345678' ID8.
     *
     * @param args Command arguments.
     */
    def config(args: String) {
        breakable {
            if (!isConnected) {
                adviseToConnect()

                break()
            }

            val argLst = parseArgs(args)

            val id8 = argValue("id8", argLst)
            val id = argValue("id", argLst)

            var node: ClusterNode = null

            if (id8.isEmpty && id.isEmpty) {
                scold("One of -id8 or -id is required.")

                break()
            }

            if (id8.isDefined && id.isDefined) {
                scold("Only one of -id8 or -id is allowed.")

                break()
            }

            if (id8.isDefined) {
                val ns = nodeById8(id8.get)

                if (ns.isEmpty) {
                    scold("Unknown 'id8' value: " + id8.get)

                    break()
                }
                else if (ns.size != 1) {
                    scold("'id8' resolves to more than one node (use full 'id' instead): " + id8.get)

                    break()
                }
                else
                    node = ns.head
            }
            else if (id.isDefined)
                try {
                    node = ignite.node(java.util.UUID.fromString(id.get))

                    if (node == null) {
                        scold("'id' does not match any node: " + id.get)

                        break()
                    }
                }
                catch {
                    case e: IllegalArgumentException =>
                        scold("Invalid node 'id': " + id.get)

                        break()
                }

            assert(node != null)

            val cfg = try
                ignite.compute(ignite.forNode(node))
                    .withNoFailover()
                    .execute(classOf[VisorNodeConfigurationCollectorTask], emptyTaskArgument(node.id()))
            catch {
                case e: IgniteException =>
                    scold(e.getMessage)

                    break()
            }

            println("Common Parameters:")

            val cmnT = VisorTextTable()

            cmnT += ("Grid name", safe(cfg.basic().gridName(), "<default>"))
            cmnT += ("Ignite home", safe(cfg.basic().ggHome(), DFLT))
            cmnT += ("Localhost", safe(cfg.basic().localHost(), DFLT))
            cmnT += ("Node ID", safe(cfg.basic().nodeId(), DFLT))
            cmnT += ("Marshaller", cfg.basic().marshaller())
            cmnT += ("Deployment mode", safe(cfg.basic().deploymentMode(), DFLT))
            cmnT += ("Daemon", bool2Str(cfg.basic().daemon()))
            cmnT += ("Remote JMX", bool2Str(cfg.basic().jmxRemote()))
            cmnT += ("Restart", bool2Str(cfg.basic().restart()))
            cmnT += ("Network timeout", cfg.basic().networkTimeout() + "ms")
            cmnT += ("Grid logger", safe(cfg.basic().logger(), DFLT))
            cmnT += ("Discovery startup delay", cfg.basic().discoStartupDelay() + "ms")
            cmnT += ("MBean server", safe(cfg.basic().mBeanServer(), DFLT))
            cmnT += ("ASCII logo disabled", bool2Str(cfg.basic().noAscii()))
            cmnT += ("Discovery order not required", bool2Str(cfg.basic().noDiscoOrder()))
            cmnT += ("Shutdown hook disabled", bool2Str(cfg.basic().noShutdownHook()))
            cmnT += ("Program name", safe(cfg.basic(). programName(), DFLT))
            cmnT += ("Quiet mode", bool2Str(cfg.basic().quiet()))
            cmnT += ("Success filename", safe(cfg.basic().successFile(), DFLT))
            cmnT += ("Update notification", bool2Str(cfg.basic().updateNotifier()))
            cmnT += ("Security credentials", safe(cfg.basic().securityCredentialsProvider(), DFLT))
            cmnT += ("Include properties", safe(cfg.includeProperties(), DFLT))

            cmnT.render()

            println("\nMetrics:")

            val metricsT = VisorTextTable()

            val expTime = cfg.metrics().expireTime()

            metricsT += ("Metrics expire time", if (expTime != Long.MaxValue) expTime + "ms" else "<never>")
            metricsT += ("Metrics history size", cfg.metrics().historySize())
            metricsT += ("Metrics log frequency", cfg.metrics().loggerFrequency())

            metricsT.render()

            println("\nSPIs:")

            val spisT = VisorTextTable()

            def spiClass(spi: IgniteBiTuple[String, java.util.Map[String, AnyRef]]) = {
                if (spi != null) spi.get2().getOrElse("Class Name", DFLT) else DFLT
            }

            def spisClass(spis: Array[IgniteBiTuple[String, java.util.Map[String, AnyRef]]]) = {
                spis.map(spiClass).mkString("[", ", ", "]")
            }

            spisT += ("Discovery", spiClass(cfg.spis().discoverySpi()))
            spisT += ("Communication", spiClass(cfg.spis().communicationSpi()))
            spisT += ("Event storage", spiClass(cfg.spis().eventStorageSpi()))
            spisT += ("Collision", spiClass(cfg.spis().collisionSpi()))
            spisT += ("Authentication", spiClass(cfg.spis().authenticationSpi()))
            spisT += ("Secure session", spiClass(cfg.spis().secureSessionSpi()))
            spisT += ("Deployment", spiClass(cfg.spis().deploymentSpi()))
            spisT += ("Checkpoints", spisClass(cfg.spis().checkpointSpis()))
            spisT += ("Failovers", spisClass(cfg.spis().failoverSpis()))
            spisT += ("Load balancings", spisClass(cfg.spis().loadBalancingSpis()))
            spisT += ("Swap spaces", spiClass(cfg.spis().swapSpaceSpi()))

            spisT.render()

            println("\nPeer-to-Peer:")

            val p2pT = VisorTextTable()

            p2pT += ("Peer class loading enabled", bool2Str(cfg.p2p().p2pEnabled()))
            p2pT += ("Missed resources cache size", cfg.p2p().p2pMissedResponseCacheSize())
            p2pT += ("Peer-to-Peer loaded packages", safe(cfg.p2p().p2pLocalClassPathExclude(), DFLT))

            p2pT.render()

            println("\nEmail:")

            val emailT = VisorTextTable()

            emailT += ("SMTP host", safe(cfg.email().smtpHost(), DFLT))
            emailT += ("SMTP port", safe(cfg.email().smtpPort(), DFLT))
            emailT += ("SMTP username", safe(cfg.email().smtpUsername(), DFLT))
            emailT += ("Admin emails", safe(cfg.email().adminEmails(), DFLT))
            emailT += ("From email", safe(cfg.email().smtpFromEmail(), DFLT))
            emailT += ("SMTP SSL enabled", bool2Str(cfg.email().smtpSsl()))
            emailT += ("SMTP STARTTLS enabled", bool2Str(cfg.email().smtpStartTls()))

            emailT.render()

            println("\nLifecycle:")

            val lifecycleT = VisorTextTable()

            lifecycleT += ("Beans", safe(cfg.lifecycle().beans(), DFLT))
            lifecycleT += ("Notifications", bool2Str(cfg.lifecycle().emailNotification()))

            lifecycleT.render()

            println("\nExecutor services:")

            val execSvcT = VisorTextTable()

            val execCfg = cfg.executeService()

            execSvcT += ("Executor service", safe(execCfg.executeService(), DFLT))
            execSvcT += ("System executor service", safe(execCfg.systemExecutorService(), DFLT))
            execSvcT += ("Peer-to-Peer executor service", safe(execCfg.p2pExecutorService(), DFLT))
            execSvcT += ("REST Executor Service", safe(execCfg.restExecutorService(), DFLT))

            execSvcT.render()

            println("\nSegmentation:")

            val segT = VisorTextTable()

            segT += ("Segmentation policy", safe(cfg.segmentation().policy(), DFLT))
            segT += ("Segmentation resolvers", safe(cfg.segmentation().resolvers(), DFLT))
            segT += ("Segmentation check frequency", cfg.segmentation().checkFrequency())
            segT += ("Wait for segmentation on start", bool2Str(cfg.segmentation().waitOnStart()))
            segT += ("All resolvers pass required", bool2Str(cfg.segmentation().passRequired()))

            segT.render()

            println("\nEvents:")

            val evtsT = VisorTextTable()

            val inclEvtTypes = Option(cfg.includeEventTypes()).fold(DFLT)(et => arr2Str(et.map(U.gridEventName)))

            evtsT += ("Included event types", inclEvtTypes)

            evtsT.render()

            println("\nREST:")

            val restT = VisorTextTable()

            restT += ("REST enabled", bool2Str(cfg.rest().restEnabled()))
            restT += ("Rest accessible folders", safe(cfg.rest().accessibleFolders(), DFLT))
            restT += ("Jetty path", safe(cfg.rest().jettyPath(), DFLT))
            restT += ("Jetty host", safe(cfg.rest().jettyHost(), DFLT))
            restT += ("Jetty port", safe(cfg.rest().jettyPort(), DFLT))
            restT += ("Tcp ssl enabled", bool2Str(cfg.rest().tcpSslEnabled()))
            restT += ("Tcp ssl context factory", safe(cfg.rest().tcpSslContextFactory(), DFLT))
            restT += ("Tcp host", safe(cfg.rest().tcpHost(), DFLT))
            restT += ("Tcp port", safe(cfg.rest().tcpPort(), DFLT))

            restT.render()

            if (cfg.userAttributes().nonEmpty) {
                println("\nUser attributes:")

                val uaT = VisorTextTable()

                uaT #= ("Name", "Value")

                cfg.userAttributes().foreach(a => uaT += (a._1, a._2))

                uaT.render()
            } else
                println("\nNo user attributes defined.")

            if (cfg.env().nonEmpty) {
                println("\nEnvironment variables:")

                val envT = VisorTextTable()

                envT.maxCellWidth = 80

                envT #= ("Name", "Value")

                cfg.env().foreach(v => envT += (v._1, compactProperty(v._1, v._2)))

                envT.render()
            } else
                println("\nNo environment variables defined.")

            val sysProps = cfg.systemProperties().toMap

            if (sysProps.nonEmpty) {
                println("\nSystem properties:")

                val spT = VisorTextTable()

                spT.maxCellWidth = 80

                spT #= ("Name", "Value")

                sysProps.foreach(p => spT += (p._1, compactProperty(p._1, p._2)))

                spT.render()
            } else
                println("\nNo system properties defined.")

            cfg.caches().foreach(cacheCfg => {
                VisorCacheCommand.showCacheConfiguration("\nCache '" + safe(cacheCfg.name(), DFLT) + "':", cacheCfg)
            })
        }
    }

    /**
     * Splits a string by path separator if it's longer than 100 characters.
     *
     * @param value String.
     * @return List of strings.
     */
    def compactProperty(name: String, value: String): List[String] = {
        val ps = getProperty("path.separator")

        // Split all values having path separator into multiple
        // lines (with few exceptions...).
        val lst =
            if (name != "path.separator" && value.indexOf(ps) != -1 && value.indexOf("http:") == -1 &&
                value.length() > 80)
                value.split(ps).toList
            else
                List(value)

        // Replace whitespaces
        lst.collect {
            case v => v.replaceAll("\n", "<NL>").replaceAll("\r", "<CR>").replaceAll("\t", "<TAB>")
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorConfigurationCommand {
    addHelp(
        name = "config",
        shortInfo = "Prints node configuration.",
        spec = List(
            "config",
            "config {-id=<node-id>|id8=<node-id8>}"
        ),
        args = List(
            "-id=<node-id>" -> List(
                "Full node ID. Either '-id8' or '-id' can be specified.",
                "If neither is specified - command starts in interactive mode."
            ),
            "-id8=<node-id8>" -> List(
                "Node ID8. Either '-id8' or '-id' can be specified.",
                "If neither is specified - command starts in interactive mode.",
                "Note you can also use '@n0' ... '@nn' variables as shortcut to <node-id>."
            )
        ),
        examples = List(
            "config -id8=12345678" ->
                "Prints configuration for node with '12345678' id8.",
            "config -id8=@n0" ->
                "Prints configuration for node with id8 taken from '@n0' memory variable.",
            "config" ->
                "Starts command in interactive mode."
        ),
        ref = VisorConsoleCommand(cmd.config, cmd.config)
    )

    /** Singleton command. */
    private val cmd = new VisorConfigurationCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromConfig2Visor(vs: VisorTag): VisorConfigurationCommand = cmd
}
