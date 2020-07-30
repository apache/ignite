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

import java.util.UUID

import org.apache.ignite.cluster.ClusterGroupEmptyException
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.internal.visor.node.{VisorGridConfiguration, VisorNodeConfigurationCollectorTask, VisorSpiDescription}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

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
        if (checkConnected()) {
            askForNode("Select node from:") match {
                case Some(id) => config("-id=" + id)
                case None => ()
            }
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
        if (checkConnected()) {
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

                    printCacheConfiguration(s"Cache '${escapeName(ccfg.getName)}':", ccfg)
                })
            }
            catch {
                case e: Throwable => scold(e)
            }
        }
    }

    @throws[ClusterGroupEmptyException]("In case of empty topology.")
    protected def collectConfiguration(nid: UUID) = {
        executeOne(nid, classOf[VisorNodeConfigurationCollectorTask], null)
    }

    protected def printConfiguration(cfg: VisorGridConfiguration) {
        println("Common Parameters:")

        val cmnT = VisorTextTable()

        val basic = cfg.getBasic

        cmnT += ("Grid name", escapeName(basic.getIgniteInstanceName))
        cmnT += ("Ignite home", safe(basic.getGgHome))
        cmnT += ("Localhost", safe(basic.getLocalHost))
        cmnT += ("Consistent ID", safe(basic.getConsistentId, "<Not configured explicitly>"))
        cmnT += ("Marshaller", basic.getMarshaller)
        cmnT += ("Deployment mode", safe(basic.getDeploymentMode))
        cmnT += ("ClientMode", javaBoolToStr(basic.isClientMode))
        cmnT += ("Daemon", bool2Str(basic.isDaemon))
        cmnT += ("Remote JMX enabled", bool2Str(basic.isJmxRemote))
        cmnT += ("Node restart enabled", bool2Str(basic.isRestart))
        cmnT += ("Network timeout", basic.getNetworkTimeout + "ms")
        cmnT += ("Grid logger", safe(basic.getLogger))
        cmnT += ("Discovery startup delay", basic.getDiscoStartupDelay + "ms")
        cmnT += ("MBean server", safe(basic.getMBeanServer))
        cmnT += ("ASCII logo disabled", bool2Str(basic.isNoAscii))
        cmnT += ("Discovery order not required", bool2Str(basic.isNoDiscoOrder))
        cmnT += ("Shutdown hook disabled", bool2Str(basic.isNoShutdownHook))
        cmnT += ("Program name", safe(basic.getProgramName))
        cmnT += ("Quiet mode", bool2Str(basic.isQuiet))
        cmnT += ("Success filename", safe(basic.getSuccessFile))
        cmnT += ("Update notification enabled", bool2Str(basic.isUpdateNotifier))
        cmnT += ("Include properties", safe(cfg.getIncludeProperties))

        val atomic = cfg.getAtomic

        cmnT += ("Atomic Cache Mode", atomic.getCacheMode)
        cmnT += ("Atomic Sequence Reservation Size", atomic.getAtomicSequenceReserveSize)
        cmnT += ("Atomic Number Of Backup Nodes", atomic.getBackups)

        val trn = cfg.getTransaction

        cmnT += ("Transaction Concurrency", trn.getDefaultTxConcurrency)
        cmnT += ("Transaction Isolation", trn.getDefaultTxIsolation)
        cmnT += ("Transaction Timeout", trn.getDefaultTxTimeout + "ms")
        cmnT += ("Transaction Log Cleanup Delay", trn.getPessimisticTxLogLinger + "ms")
        cmnT += ("Transaction Log Size", trn.getPessimisticTxLogSize)
        cmnT += ("Transaction Manager Factory", trn.getTxManagerFactory)
        cmnT += ("Transaction Use JTA", bool2Str(trn.isUseJtaSync))

        cmnT.render()

        println("\nMetrics:")

        val metricsT = VisorTextTable()

        val metricsCfg = cfg.getMetrics

        val expTime = metricsCfg.getExpireTime

        metricsT += ("Metrics expire time", if (expTime != Long.MaxValue) expTime + "ms" else "<never>")
        metricsT += ("Metrics history size", metricsCfg.getHistorySize)
        metricsT += ("Metrics log frequency", metricsCfg.getLoggerFrequency)

        metricsT.render()

        println("\nSPIs:")

        val spisT = VisorTextTable()

        def spiClass(spi: VisorSpiDescription) = {
            if (spi != null) spi.getFieldDescriptions.getOrElse("Class Name", NA) else NA
        }

        def spisClass(spis: Array[VisorSpiDescription]) = {
            spis.map(spiClass).mkString("[", ", ", "]")
        }

        val spisCfg = cfg.getSpis

        spisT += ("Discovery", spiClass(spisCfg.getDiscoverySpi))
        spisT += ("Communication", spiClass(spisCfg.getCommunicationSpi))
        spisT += ("Event storage", spiClass(spisCfg.getEventStorageSpi))
        spisT += ("Collision", spiClass(spisCfg.getCollisionSpi))
        spisT += ("Deployment", spiClass(spisCfg.getDeploymentSpi))
        spisT += ("Checkpoints", spisClass(spisCfg.getCheckpointSpis))
        spisT += ("Failovers", spisClass(spisCfg.getFailoverSpis))
        spisT += ("Load balancings", spisClass(spisCfg.getLoadBalancingSpis))
        spisT += ("Indexing", spisClass(spisCfg.getIndexingSpis))

        spisT.render()

        println("\nClient connector configuration")

        val cliConnCfg = cfg.getClientConnectorConfiguration
        val cliConnTbl = VisorTextTable()

        if (cliConnCfg != null) {
            cliConnTbl += ("Host", safe(cliConnCfg.getHost, safe(basic.getLocalHost)))
            cliConnTbl += ("Port", cliConnCfg.getPort)
            cliConnTbl += ("Port range", cliConnCfg.getPortRange)
            cliConnTbl += ("Socket send buffer size", formatMemory(cliConnCfg.getSocketSendBufferSize))
            cliConnTbl += ("Socket receive buffer size", formatMemory(cliConnCfg.getSocketReceiveBufferSize))
            cliConnTbl += ("Max connection cursors", cliConnCfg.getMaxOpenCursorsPerConnection)
            cliConnTbl += ("Pool size", cliConnCfg.getThreadPoolSize)
            cliConnTbl += ("Idle Timeout", cliConnCfg.getIdleTimeout + "ms")
            cliConnTbl += ("TCP_NODELAY", bool2Str(cliConnCfg.isTcpNoDelay))
            cliConnTbl += ("JDBC Enabled", bool2Str(cliConnCfg.isJdbcEnabled))
            cliConnTbl += ("ODBC Enabled", bool2Str(cliConnCfg.isOdbcEnabled))
            cliConnTbl += ("Thin Client Enabled", bool2Str(cliConnCfg.isThinClientEnabled))
            cliConnTbl += ("SSL Enabled", bool2Str(cliConnCfg.isSslEnabled))
            cliConnTbl += ("Ssl Client Auth", bool2Str(cliConnCfg.isSslClientAuth))
            cliConnTbl += ("Use Ignite SSL Context Factory", bool2Str(cliConnCfg.isUseIgniteSslContextFactory))
            cliConnTbl += ("SSL Context Factory", safe(cliConnCfg.getSslContextFactory))

            cliConnTbl.render()
        }
        else
            println("Client Connection is not configured")

        println("\nPeer-to-Peer:")

        val p2pT = VisorTextTable()

        val p2pCfg = cfg.getP2p

        p2pT += ("Peer class loading enabled", bool2Str(p2pCfg.isPeerClassLoadingEnabled))
        p2pT += ("Missed resources cache size", p2pCfg.getPeerClassLoadingMissedResourcesCacheSize)
        p2pT += ("Peer-to-Peer loaded packages", safe(p2pCfg.getPeerClassLoadingLocalClassPathExclude))

        p2pT.render()

        println("\nLifecycle:")

        val lifecycleT = VisorTextTable()

        lifecycleT += ("Beans", safe(cfg.getLifecycle.getBeans))

        lifecycleT.render()

        println("\nExecutor services:")

        val execSvcT = VisorTextTable()

        val execCfg = cfg.getExecutorService

        execSvcT += ("Public thread pool size", safe(execCfg.getPublicThreadPoolSize))
        execSvcT += ("System thread pool size", safe(execCfg.getSystemThreadPoolSize))
        execSvcT += ("Management thread pool size", safe(execCfg.getManagementThreadPoolSize))
        execSvcT += ("Peer-to-Peer thread pool size", safe(execCfg.getPeerClassLoadingThreadPoolSize))
        execSvcT += ("Rebalance Thread Pool size", execCfg.getRebalanceThreadPoolSize)
        execSvcT += ("REST thread pool size", safe(execCfg.getRestThreadPoolSize))
        execSvcT += ("Client connector thread pool size", safe(execCfg.getClientConnectorConfigurationThreadPoolSize))

        execSvcT.render()

        println("\nSegmentation:")

        val segT = VisorTextTable()

        val segmentationCfg = cfg.getSegmentation

        segT += ("Segmentation policy", safe(segmentationCfg.getPolicy))
        segT += ("Segmentation resolvers", safe(segmentationCfg.getResolvers))
        segT += ("Segmentation check frequency", segmentationCfg.getCheckFrequency)
        segT += ("Wait for segmentation on start", bool2Str(segmentationCfg.isWaitOnStart))
        segT += ("All resolvers pass required", bool2Str(segmentationCfg.isAllSegmentationResolversPassRequired))

        segT.render()

        println("\nEvents:")

        val evtsT = VisorTextTable()

        val inclEvtTypes = Option(cfg.getIncludeEventTypes).fold(NA)(et => arr2Str(et.map(U.gridEventName)))

        evtsT += ("Included event types", inclEvtTypes)

        evtsT.render()

        println("\nREST:")

        val restT = VisorTextTable()

        val restCfg = cfg.getRest

        restT += ("REST enabled", bool2Str(restCfg.isRestEnabled))
        restT += ("Jetty path", safe(restCfg.getJettyPath))
        restT += ("Jetty host", safe(restCfg.getJettyHost))
        restT += ("Jetty port", safe(restCfg.getJettyPort))
        restT += ("Tcp ssl enabled", bool2Str(restCfg.isTcpSslEnabled))
        restT += ("Tcp ssl context factory", safe(restCfg.getTcpSslContextFactory))
        restT += ("Tcp host", safe(restCfg.getTcpHost))
        restT += ("Tcp port", safe(restCfg.getTcpPort))

        restT.render()

        if (cfg.getUserAttributes.nonEmpty) {
            println("\nUser attributes:")

            val uaT = VisorTextTable()

            uaT #= ("Name", "Value")

            cfg.getUserAttributes.foreach(a => uaT += (a._1, a._2))

            uaT.render()
        } else
            println("\nNo user attributes defined.")

        if (cfg.getEnv.nonEmpty) {
            println("\nEnvironment variables:")

            val envT = VisorTextTable()

            envT.maxCellWidth = 80

            envT #= ("Name", "Value")

            cfg.getEnv.foreach(v => envT += (v._1, compactProperty(v._1, v._2)))

            envT.render()
        } else
            println("\nNo environment variables defined.")

        val sysProps = cfg.getSystemProperties.toMap

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
        val ps = System.getProperty("path.separator")

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
