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

import org.apache.ignite.cluster.ClusterGroupEmptyException
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.lang.IgniteBiTuple
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import java.lang.System._
import java.util.UUID

import org.apache.ignite.internal.visor.node.{VisorGridConfiguration, VisorNodeConfigurationCollectorTask}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * ==Overview==
 * Visor 'config' command implementation.
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
 *     -id8=<node-id8>
 *         Node ID8.
 *         Note that either '-id8' or '-id' should be specified.
 *         You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.
 *         To specify oldest node on the same host as visor use variable '@nl'.
 *         To specify oldest node on other hosts that are not running visor use variable '@nr'.
 *         If neither is specified - command starts in interactive mode.
 *     -id=<node-id>
 *         Full node ID. Either '-id8' or '-id' can be specified.
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
class VisorConfigurationCommand extends VisorConsoleCommand {
    @impl protected val name = "config"

    /**
      * ===Command===
      * Run command in interactive mode.
      *
      * ===Examples===
      * <ex>config</ex>
      * Starts command in interactive mode.
     */
    def config() {
        if (isConnected)
            askForNode("Select node from:") match {
                case Some(id) => config("-id=" + id)
                case None => ()
            }
        else
            adviseToConnect()
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
        if (!isConnected) {
            adviseToConnect()

            return
        }

        val argLst = parseArgs(args)

        val nid = parseNode(argLst) match {
            case Left(msg) =>
                scold(msg)

                return

            case Right(None) =>
                scold("One of -id8 or -id is required.")

                return

            case Right(Some(n)) =>
                assert(n != null)

                n.id()
        }

        try {
            val cfg = collectConfiguration(nid)

            printConfiguration(cfg)

            cacheConfigurations(nid).foreach(ccfg => {
                println()

                printCacheConfiguration(s"Cache '${escapeName(ccfg.name())}':", ccfg)
            })
        }
        catch {
            case e: Throwable => scold(e)
        }
    }

    @throws[ClusterGroupEmptyException]("In case of empty topology.")
    protected def collectConfiguration(nid: UUID) = {
        executeOne(nid, classOf[VisorNodeConfigurationCollectorTask], null)
    }

    protected def printConfiguration(cfg: VisorGridConfiguration) {
        println("Common Parameters:")

        val cmnT = VisorTextTable()

        val basic = cfg.basic()

        cmnT += ("Grid name", escapeName(basic.gridName()))
        cmnT += ("Ignite home", safe(basic.ggHome()))
        cmnT += ("Localhost", safe(basic.localHost()))
        cmnT += ("Node ID", safe(basic.nodeId()))
        cmnT += ("Marshaller", basic.marshaller())
        cmnT += ("Deployment mode", safe(basic.deploymentMode()))
        cmnT += ("ClientMode", javaBoolToStr(basic.clientMode()))
        cmnT += ("Daemon", bool2Str(basic.daemon()))
        cmnT += ("Remote JMX", bool2Str(basic.jmxRemote()))
        cmnT += ("Restart", bool2Str(basic.restart()))
        cmnT += ("Network timeout", basic.networkTimeout() + "ms")
        cmnT += ("Grid logger", safe(basic.logger()))
        cmnT += ("Discovery startup delay", basic.discoStartupDelay() + "ms")
        cmnT += ("MBean server", safe(basic.mBeanServer()))
        cmnT += ("ASCII logo disabled", bool2Str(basic.noAscii()))
        cmnT += ("Discovery order not required", bool2Str(basic.noDiscoOrder()))
        cmnT += ("Shutdown hook disabled", bool2Str(basic.noShutdownHook()))
        cmnT += ("Program name", safe(basic. programName()))
        cmnT += ("Quiet mode", bool2Str(basic.quiet()))
        cmnT += ("Success filename", safe(basic.successFile()))
        cmnT += ("Update notification", bool2Str(basic.updateNotifier()))
        cmnT += ("Include properties", safe(cfg.includeProperties()))

        val atomic = cfg.atomic()

        cmnT += ("Atomic Cache Mode", atomic.cacheMode())
        cmnT += ("Atomic Sequence Reservation Size", atomic.atomicSequenceReserveSize())
        cmnT += ("Atomic Number Of Backup Nodes", atomic.backups())

        val trn = cfg.transaction()

        cmnT += ("Transaction Concurrency", trn.defaultTxConcurrency())
        cmnT += ("Transaction Isolation", trn.defaultTxIsolation())
        cmnT += ("Transaction Timeout", trn.defaultTxTimeout() + "ms")
        cmnT += ("Transaction Log Cleanup Delay", trn.pessimisticTxLogLinger() + "ms")
        cmnT += ("Transaction Log Size", trn.getPessimisticTxLogSize)
        cmnT += ("Transaction Serializable Enabled", bool2Str(trn.txSerializableEnabled()))

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
            if (spi != null) spi.get2().getOrElse("Class Name", NA) else NA
        }

        def spisClass(spis: Array[IgniteBiTuple[String, java.util.Map[String, AnyRef]]]) = {
            spis.map(spiClass).mkString("[", ", ", "]")
        }

        spisT += ("Discovery", spiClass(cfg.spis().discoverySpi()))
        spisT += ("Communication", spiClass(cfg.spis().communicationSpi()))
        spisT += ("Event storage", spiClass(cfg.spis().eventStorageSpi()))
        spisT += ("Collision", spiClass(cfg.spis().collisionSpi()))
        spisT += ("Deployment", spiClass(cfg.spis().deploymentSpi()))
        spisT += ("Checkpoints", spisClass(cfg.spis().checkpointSpis()))
        spisT += ("Failovers", spisClass(cfg.spis().failoverSpis()))
        spisT += ("Load balancings", spisClass(cfg.spis().loadBalancingSpis()))
        spisT += ("Swap spaces", spiClass(cfg.spis().swapSpaceSpi()))
        spisT += ("Indexing", spisClass(cfg.spis().indexingSpis()))

        spisT.render()

        println("\nPeer-to-Peer:")

        val p2pT = VisorTextTable()

        p2pT += ("Peer class loading enabled", bool2Str(cfg.p2p().p2pEnabled()))
        p2pT += ("Missed resources cache size", cfg.p2p().p2pMissedResponseCacheSize())
        p2pT += ("Peer-to-Peer loaded packages", safe(cfg.p2p().p2pLocalClassPathExclude()))

        p2pT.render()

        println("\nLifecycle:")

        val lifecycleT = VisorTextTable()

        lifecycleT += ("Beans", safe(cfg.lifecycle().beans()))

        lifecycleT.render()

        println("\nExecutor services:")

        val execSvcT = VisorTextTable()

        val execCfg = cfg.executeService()

        execSvcT += ("Public thread pool size", safe(execCfg.publicThreadPoolSize()))
        execSvcT += ("System thread pool size", safe(execCfg.systemThreadPoolSize()))
        execSvcT += ("Management thread pool size", safe(execCfg.managementThreadPoolSize()))
        execSvcT += ("IGFS thread pool size", safe(execCfg.igfsThreadPoolSize()))
        execSvcT += ("Peer-to-Peer thread pool size", safe(execCfg.peerClassLoadingThreadPoolSize()))
        execSvcT += ("REST thread pool size", safe(execCfg.restThreadPoolSize()))

        execSvcT.render()

        println("\nSegmentation:")

        val segT = VisorTextTable()

        segT += ("Segmentation policy", safe(cfg.segmentation().policy()))
        segT += ("Segmentation resolvers", safe(cfg.segmentation().resolvers()))
        segT += ("Segmentation check frequency", cfg.segmentation().checkFrequency())
        segT += ("Wait for segmentation on start", bool2Str(cfg.segmentation().waitOnStart()))
        segT += ("All resolvers pass required", bool2Str(cfg.segmentation().passRequired()))

        segT.render()

        println("\nEvents:")

        val evtsT = VisorTextTable()

        val inclEvtTypes = Option(cfg.includeEventTypes()).fold(NA)(et => arr2Str(et.map(U.gridEventName)))

        evtsT += ("Included event types", inclEvtTypes)

        evtsT.render()

        println("\nREST:")

        val restT = VisorTextTable()

        restT += ("REST enabled", bool2Str(cfg.rest().restEnabled()))
        restT += ("Rest accessible folders", safe(cfg.rest().accessibleFolders()))
        restT += ("Jetty path", safe(cfg.rest().jettyPath()))
        restT += ("Jetty host", safe(cfg.rest().jettyHost()))
        restT += ("Jetty port", safe(cfg.rest().jettyPort()))
        restT += ("Tcp ssl enabled", bool2Str(cfg.rest().tcpSslEnabled()))
        restT += ("Tcp ssl context factory", safe(cfg.rest().tcpSslContextFactory()))
        restT += ("Tcp host", safe(cfg.rest().tcpHost()))
        restT += ("Tcp port", safe(cfg.rest().tcpPort()))

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
    }

    /**
     * Splits a string by path separator if it's longer than 100 characters.
     *
     * @param value String.
     * @return List of strings.
     */
    private[this] def compactProperty(name: String, value: String): List[String] = {
        val ps = getProperty("path.separator")

        // Split all values having path separator into multiple lines (with few exceptions...).
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
    /** Singleton command. */
    private val cmd = new VisorConfigurationCommand

    addHelp(
        name = cmd.name,
        shortInfo = "Prints node configuration.",
        spec = List(
            cmd.name,
            s"${cmd.name} {-id=<node-id>|id8=<node-id8>}"
        ),
        args = List(
            "-id8=<node-id8>" -> List(
                "Node ID8.",
                "Note that either '-id8' or '-id' should be specified.",
                "You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.",
                "To specify oldest node on the same host as visor use variable '@nl'.",
                "To specify oldest node on other hosts that are not running visor use variable '@nr'.",
                "If neither is specified - command starts in interactive mode."
            ),
            "-id=<node-id>" -> List(
                "Full node ID. Either '-id8' or '-id' can be specified.",
                "If neither is specified - command starts in interactive mode."
            )
        ),
        examples = List(
            s"${cmd.name} -id8=12345678" ->
                "Prints configuration for node with '12345678' id8.",
            s"${cmd.name} -id8=@n0" ->
                "Prints configuration for node with id8 taken from '@n0' memory variable.",
            cmd.name ->
                "Starts command in interactive mode."
        ),
        emptyArgs = cmd.config,
        withArgs = cmd.config
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
    implicit def fromConfig2Visor(vs: VisorTag): VisorConfigurationCommand = cmd
}
