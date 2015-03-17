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

package org.apache.ignite.visor.commands.cache

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.util.typedef._
import org.apache.ignite.internal.visor.cache._
import org.apache.ignite.internal.visor.node.{VisorGridConfiguration, VisorNodeConfigurationCollectorTask}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.lang.IgniteBiTuple
import org.jetbrains.annotations._

import java.lang.{Boolean => JavaBoolean}
import java.util.UUID

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'cache' command implementation.
 *
 * ==Help==
 * {{{
 * +-----------------------------------------------------------------------------------------+
 * | cache          | Prints statistics about caches from specified node on the entire grid. |
 * |                | Output sorting can be specified in arguments.                          |
 * |                |                                                                        |
 * |                | Output abbreviations:                                                  |
 * |                |     #   Number of nodes.                                               |
 * |                |     H/h Number of cache hits.                                          |
 * |                |     M/m Number of cache misses.                                        |
 * |                |     R/r Number of cache reads.                                         |
 * |                |     W/w Number of cache writes.                                        |
 * +-----------------------------------------------------------------------------------------+
 * | cache -clear   | Clears all entries from cache on all nodes.                            |
 * +-----------------------------------------------------------------------------------------+
 * | cache -scan    | List all entries in cache with specified name.                         |
 * +-----------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache
 *     cache -i
 *     cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr|cn} {-a} {-r}
 *     cache -clear {-c=<cache-name>}
 *     cache -scan -c=<cache-name> {-id=<node-id>|id8=<node-id8>} {-p=<page size>}
 *     cache -swap {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full ID of the node to get cache statistics from.
 *         Either '-id8' or '-id' can be specified.
 *         If neither is specified statistics will be gathered from all nodes.
 *     -id8=<node-id>
 *         ID8 of the node to get cache statistics from.
 *         Either '-id8' or '-id' can be specified.
 *         If neither is specified statistics will be gathered from all nodes.
 *     -c=<cache-name>
 *         Name of the cache.
 *     -s=hi|mi|rd|wr|cn
 *         Defines sorting type. Sorted by:
 *            hi Hits.
 *            mi Misses.
 *            rd Reads.
 *            wr Writes.
 *            cn Cache name.
 *         If not specified - default sorting is 'cn'.
 *     -i
 *         Interactive mode.
 *         User can interactively select node for cache statistics.
 *     -r
 *         Defines if sorting should be reversed.
 *         Can be specified only with '-s' argument.
 *     -a
 *         Prints details statistics about each cache.
 *         By default only aggregated summary is printed.
 *     -clear
 *          Clears cache.
 *     -scan
 *          Prints list of all entries from cache.
 *     -swap
 *          Swaps backup entries in cache.
 *     -p=<page size>
 *         Number of object to fetch from cache at once.
 *         Valid range from 1 to 100.
 *         By default page size is 25.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cache
 *         Prints summary statistics about all caches.
 *     cache -id8=12345678 -s=hi -r
 *         Prints summary statistics about caches from node with specified id8
 *         sorted by number of hits in reverse order.
 *     cache -i
 *         Prints cache statistics for interactively selected node.
 *     cache -s=hi -r -a
 *         Prints detailed statistics about all caches sorted by number of hits in reverse order.
 *     cache -clear
 *         Clears interactively selected cache.
 *     cache -clear -c=cache
 *         Clears cache with name 'cache'.
 *     cache -scan
 *         Prints list entries from interactively selected cache.
 *     cache -scan -c=cache
 *         Prints list entries from cache with name 'cache' from all nodes with this cache.
 *     cache -scan -c=@c0 -p=50
 *         Prints list entries from cache with name taken from 'c0' memory variable
 *         with page of 50 items from all nodes with this cache.
 *     cache -scan -c=cache -id8=12345678
 *         Prints list entries from cache with name 'cache' and node '12345678' ID8.
 *     cache -swap
 *         Swaps entries in interactively selected cache.
 *     cache -swap -c=cache
 *         Swaps entries in cache with name 'cache'.
 *     cache -swap -c=@c0
 *         Swaps entries in cache with name taken from 'c0' memory variable.
 * }}}
 */
class VisorCacheCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cache' to see how to use this command.")
    }

    /**
     * ===Command===
     * Prints statistics about caches from nodes that pass mnemonic predicate.
     * Sorting can be specified in arguments.
     *
     * ===Examples===
     * <ex>cache -id8=12345678 -s=no -r</ex>
     *     Prints statistics about caches from node with specified id8 sorted by number of nodes in reverse order.
     * <br>
     * <ex>cache -s=no -r</ex>
     *     Prints statistics about all caches sorted by number of nodes in reverse order.
     * <br>
     * <ex>cache -clear</ex>
     *      Clears interactively selected cache.
     * <br>
     * <ex>cache -clear -c=cache</ex>
     *      Clears cache with name 'cache'.
     * <br>
     * <ex>cache -scan</ex>
     *     Prints list entries from interactively selected cache.
     * <br>
     * <ex>cache -scan -c=cache</ex>
     *     Prints list entries from cache with name 'cache' from all nodes with this cache.
     * <br>
     * <ex>cache -scan -c=@c0 -p=50</ex>
     *     Prints list entries from cache with name taken from 'c0' memory variable with page of 50 items
     *     from all nodes with this cache.
     * <br>
     * <ex>cache -scan -c=cache -id8=12345678</ex>
     *     Prints list entries from cache with name 'cache' and node '12345678' ID8.
     * <br>
     * <ex>cache -swap</ex>
     *     Swaps entries in interactively selected cache.
     * <br>
     * <ex>cache -swap -c=cache</ex>
     *     Swaps entries in cache with name 'cache'.
     * <br>
     * <ex>cache -swap -c=@c0</ex>
     *     Swaps entries in cache with name taken from 'c0' memory variable.
     *
     * @param args Command arguments.
     */
    def cache(args: String) {
        breakable {
            if (!isConnected)
                adviseToConnect()
            else {
                var argLst = parseArgs(args)

                if (hasArgFlag("i", argLst)) {
                    askForNode("Select node from:") match {
                        case Some(nid) => ask("Detailed statistics (y/n) [n]: ", "n") match {
                            case "n" | "N" => nl(); cache("-id=" + nid).^^
                            case "y" | "Y" => nl(); cache("-a -id=" + nid).^^
                            case x => nl(); warn("Invalid answer: " + x).^^
                        }
                        case None => break()
                    }

                    break()
                }

                val node = parseNode(argLst) match {
                    case Left(msg) =>
                        scold(msg)

                        break()

                    case Right(n) => n
                }

                val cacheName = argValue("c", argLst) match {
                    case Some(dfltName) if dfltName == DFLT_CACHE_KEY || dfltName == DFLT_CACHE_NAME =>
                        argLst = argLst.filter(_._1 != "c") ++ Seq("c" -> null)

                        Some(null)

                    case cn => cn
                }

                if (Seq("clear", "swap", "scan").exists(hasArgFlag(_, argLst))) {
                    if (cacheName.isEmpty)
                        askForCache("Select cache from:", node) match {
                            case Some(name) => argLst = argLst ++ Seq("c" -> name)
                            case None => break()
                        }

                    if (hasArgFlag("clear", argLst))
                        VisorCacheClearCommand().clear(argLst, node)
                    else if (hasArgFlag("swap", argLst))
                        VisorCacheSwapCommand().swap(argLst, node)
                    else if (hasArgFlag("scan", argLst))
                        VisorCacheScanCommand().scan(argLst, node)

                    break()
                }

                val all = hasArgFlag("a", argLst)

                val sortType = argValue("s", argLst)
                val reversed = hasArgName("r", argLst)

                if (sortType.isDefined && !isValidSortType(sortType.get))
                    scold("Invalid '-s' argument in: " + args).^^

                // Get cache stats data from all nodes.
                val aggrData = cacheData(node, cacheName)

                if (aggrData.isEmpty)
                    scold("No caches found.").^^

                println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

                val sumT = VisorTextTable()

                sumT #= ("Name(@)", "Nodes", "Entries", "Hits", "Misses", "Reads", "Writes")

                sortAggregatedData(aggrData, sortType.getOrElse("cn"), reversed).foreach(
                    ad => {
                        // Add cache host as visor variable.
                        registerCacheName(ad.cacheName)

                        sumT += (
                            mkCacheName(ad.cacheName),
                            ad.nodes,
                            (
                                "min: " + ad.minimumSize,
                                "avg: " + formatDouble(ad.averageSize),
                                "max: " + ad.maximumSize
                                ),
                            (
                                "min: " + ad.minimumHits,
                                "avg: " + formatDouble(ad.averageHits),
                                "max: " + ad.maximumHits
                                ),
                            (
                                "min: " + ad.minimumMisses,
                                "avg: " + formatDouble(ad.averageMisses),
                                "max: " + ad.maximumMisses
                                ),
                            (
                                "min: " + ad.minimumReads,
                                "avg: " + formatDouble(ad.averageReads),
                                "max: " + ad.maximumReads
                                ),
                            (
                                "min: " + ad.minimumWrites,
                                "avg: " + formatDouble(ad.averageWrites),
                                "max: " + ad.maximumWrites
                                )
                            )
                    }
                )

                sumT.render()

                if (all) {
                    val sorted = aggrData.sortWith((k1, k2) => {
                        if (k1.cacheName == null)
                            true
                        else if (k2.cacheName == null)
                            false
                        else k1.cacheName.compareTo(k2.cacheName) < 0
                    })

                    val gCfg = node.map(config).collect {
                        case cfg if cfg != null => cfg
                    }

                    sorted.foreach(ad => {
                        val cacheNameVar = mkCacheName(ad.cacheName)

                        println("\nCache '" + cacheNameVar + "':")

                        val m = ad.metrics()

                        val csT = VisorTextTable()

                        csT += ("Name(@)", cacheNameVar)
                        csT += ("Nodes", m.size())
                        csT += ("Size Min/Avg/Max", ad.minimumSize + " / " + formatDouble(ad.averageSize) + " / " + ad.maximumSize)

                        val ciT = VisorTextTable()

                        ciT #= ("Node ID8(@), IP", "CPUs", "Heap Used", "CPU Load", "Up Time", "Size", "Hi/Mi/Rd/Wr")

                        sortData(m.toMap, sortType.getOrElse("hi"), reversed).foreach { case (nid, cm) =>
                            val nm = ignite.cluster.node(nid).metrics()

                            ciT += (
                                nodeId8Addr(nid),
                                nm.getTotalCpus,
                                formatDouble(nm.getHeapMemoryUsed / nm.getHeapMemoryMaximum * 100.0d) + " %",

                                formatDouble(nm.getCurrentCpuLoad * 100.0) + " %",
                                X.timeSpan2HMSM(nm.getUpTime),
                                cm.keySize(),
                                (
                                    "Hi: " + cm.hits(),
                                    "Mi: " + cm.misses(),
                                    "Rd: " + cm.reads(),
                                    "Wr: " + cm.writes()
                                )
                            )
                        }

                        csT.render()

                        nl()
                        println("Nodes for: " + cacheNameVar)

                        ciT.render()

                        // Print footnote.
                        println("'Hi' - Number of cache hits.")
                        println("'Mi' - Number of cache misses.")
                        println("'Rd' - number of cache reads.")
                        println("'Wr' - Number of cache writes.")

                        // Print metrics.
                        nl()
                        println("Aggregated queries metrics:")
                        println("  Minimum execution time: " + X.timeSpan2HMSM(ad.minimumQueryTime()))
                        println("  Maximum execution time: " + X.timeSpan2HMSM(ad.maximumQueryTime))
                        println("  Average execution time: " + X.timeSpan2HMSM(ad.averageQueryTime.toLong))
                        println("  Total number of executions: " + ad.execsQuery)
                        println("  Total number of failures:   " + ad.failsQuery)

                        gCfg.foreach(_.caches().find(_.name() == ad.cacheName()).foreach(cfg => {
                            nl()

                            showCacheConfiguration("Cache configuration:", cfg)
                        }))
                    })

                }
                else
                    println("\nUse \"-a\" flag to see detailed statistics.")
            }
        }
    }

    /**
     * Makes extended cache host attaching optional visor variable host
     * associated with it.
     *
     * @param s Cache host.
     */
    private def mkCacheName(@Nullable s: String): String = {
        if (s == null) {
            val v = mfind(DFLT_CACHE_KEY)

            DFLT_CACHE_NAME + (if (v.isDefined) "(@" + v.get._1 + ')' else "")
        }
        else {
            val v = mfind(s)

            s + (if (v.isDefined) "(@" + v.get._1 + ')' else "")
        }
    }

    /**
     * Registers cache host as a visor variable if one wasn't already registered.
     *
     * @param s Cache host.
     */
    private def registerCacheName(@Nullable s: String) = setVarIfAbsent(if (s != null) s else DFLT_CACHE_KEY, "c")

    /**
     * ===Command===
     * Prints unsorted statistics about all caches.
     *
     * ===Examples===
     * <ex>cache</ex>
     * Prints unsorted statistics about all caches.
     */
    def cache() {
        this.cache("")
    }

    /**
     * Get metrics data for all caches from all node or from specified node.
     *
     * @return Caches metrics data.
     */
    private def cacheData(node: Option[ClusterNode], name: Option[String]): List[VisorCacheAggregatedMetrics] = {
        assert(node != null)

        try {
            val prj = node.fold(ignite.cluster.forRemotes())(ignite.cluster.forNode(_))

            val nids = prj.nodes().map(_.id())

            ignite.compute(prj).execute(classOf[VisorCacheMetricsCollectorTask], toTaskArgument(nids,
                new IgniteBiTuple(JavaBoolean.valueOf(name.isEmpty), name.orNull))).toList
        }
        catch {
            case e: IgniteException => Nil
        }
    }

    /**
     * Gets configuration of grid from specified node for callecting of node cache's configuration.
     *
     * @param node Specified node.
     * @return Grid configuration for specified node.
     */
    private def config(node: ClusterNode): VisorGridConfiguration = {
        try
            ignite.compute(ignite.cluster.forNode(node)).withNoFailover()
                .execute(classOf[VisorNodeConfigurationCollectorTask], emptyTaskArgument(node.id()))
        catch {
            case e: IgniteException =>
                scold(e.getMessage)

                null
        }
    }

    /**
     * Tests whether passed in parameter is a valid sorting type.
     *
     * @param arg Sorting type to test.
     */
    private def isValidSortType(arg: String): Boolean = {
        assert(arg != null)

        Set("hi", "mi", "rd", "wr", "cn").contains(arg.trim)
    }

    /**
     * Sort metrics data.
     *
     * @param data Unsorted list.
     * @param arg Sorting command argument.
     * @param reverse Whether to reverse sorting or not.
     * @return Sorted data.
     */
    private def sortData(data: Map[UUID, VisorCacheMetrics], arg: String, reverse: Boolean) = {
        assert(data != null)
        assert(arg != null)

        val sorted = arg.trim match {
            case "hi" => data.toSeq.sortBy(_._2.hits)
            case "mi" => data.toSeq.sortBy(_._2.misses)
            case "rd" => data.toSeq.sortBy(_._2.reads)
            case "wr" => data.toSeq.sortBy(_._2.writes)
            case "cn" => data.toSeq.sortBy(_._1)

            case _ =>
                assert(false, "Unknown sorting type: " + arg)

                Nil
        }

        if (reverse) sorted.reverse else sorted
    }

    /**
     * Sort aggregated metrics data.
     *
     * @param data Unsorted list.
     * @param arg Command argument.
     * @param reverse Whether to reverse sorting or not.
     * @return Sorted data.
     */
    private def sortAggregatedData(data: Iterable[VisorCacheAggregatedMetrics], arg: String, reverse: Boolean):
        List[VisorCacheAggregatedMetrics] = {

        val sorted = arg.trim match {
            case "hi" => data.toList.sortBy(_.averageHits)
            case "mi" => data.toList.sortBy(_.averageMisses)
            case "rd" => data.toList.sortBy(_.averageReads)
            case "wr" => data.toList.sortBy(_.averageWrites)
            case "cn" => data.toList.sortWith((x, y) =>
                x.cacheName == null || (y.cacheName != null && x.cacheName.toLowerCase < y.cacheName.toLowerCase))

            case _ =>
                assert(false, "Unknown sorting type: " + arg)

                Nil
        }

        if (reverse) sorted.reverse else sorted
    }

    /**
     * Asks user to select a cache from the list.
     *
     * @param title Title displayed before the list of caches.
     * @return `Option` for ID of selected cache.
     */
    def askForCache(title: String, node: Option[ClusterNode]): Option[String] = {
        assert(title != null)
        assert(visor.visor.isConnected)

        // Get cache stats data from all nodes.
        val aggrData = cacheData(node, None)

        if (aggrData.isEmpty)
            scold("No caches found.").^^

        val sortedAggrData = sortAggregatedData(aggrData, "cn", false)

        println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

        val sumT = VisorTextTable()

        sumT #= ("#", "Name(@)", "Nodes", "Size")

        (0 until sortedAggrData.size) foreach (i => {
            val ad = sortedAggrData(i)

            // Add cache host as visor variable.
            registerCacheName(ad.cacheName)

            sumT += (
                i,
                mkCacheName(ad.cacheName),
                ad.nodes,
                (
                    "min: " + ad.minimumSize,
                    "avg: " + formatDouble(ad.averageSize),
                    "max: " + ad.maximumSize
                ))
        })

        sumT.render()

        val a = ask("\nChoose cache number ('c' to cancel) [c]: ", "c")

        if (a.toLowerCase == "c")
            None
        else {
            try
                Some(sortedAggrData(a.toInt).cacheName)
            catch {
                case e: Throwable =>
                    warn("Invalid selection: " + a)

                    None
            }
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheCommand {
    addHelp(
        name = "cache",
        shortInfo = "Prints cache statistics, clears cache, prints list of all entries from cache.",
        longInfo = Seq(
            "Prints statistics about caches from specified node on the entire grid.",
            "Output sorting can be specified in arguments.",
            " ",
            "Output abbreviations:",
            "    #   Number of nodes.",
            "    H/h Number of cache hits.",
            "    M/m Number of cache misses.",
            "    R/r Number of cache reads.",
            "    W/w Number of cache writes.",
            " ",
            "Clears cache.",
            " ",
            "Prints list of all entries from cache.",
            " ",
            "Swaps backup entries in cache."
        ),
        spec = Seq(
            "cache",
            "cache -i",
            "cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr} {-a} {-r}",
            "cache -clear {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}",
            "cache -scan -c=<cache-name> {-id=<node-id>|id8=<node-id8>} {-p=<page size>}",
            "cache -swap {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}"
    ),
        args = Seq(
            "-id=<node-id>" -> Seq(
                "Full ID of the node to get cache statistics from.",
                "Either '-id8' or '-id' can be specified.",
                "If neither is specified statistics will be gathered from all nodes."
            ),
            "-id8=<node-id>" -> Seq(
                "ID8 of the node to get cache statistics from.",
                "Either '-id8' or '-id' can be specified.",
                "If neither is specified statistics will be gathered from all nodes.",
                "Note you can also use '@n0' ... '@nn' variables as shortcut to <node-id>."
            ),
            "-c=<cache-name>" -> Seq(
                "Name of the cache.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            ),
            "-clear" -> Seq(
                "Clears cache."
            ),
            "-scan" -> Seq(
                "Prints list of all entries from cache."
            ),
            "-swap" -> Seq(
                "Swaps backup entries in cache."
            ),
            "-s=hi|mi|rd|wr|cn" -> Seq(
                "Defines sorting type. Sorted by:",
                "   hi Hits.",
                "   mi Misses.",
                "   rd Reads.",
                "   wr Writes.",
                "   cn Cache name.",
                "If not specified - default sorting is 'cn'."
            ),
            "-i" -> Seq(
                "Interactive mode.",
                "User can interactively select node for cache statistics."
            ),
            "-r" -> Seq(
                "Defines if sorting should be reversed.",
                "Can be specified only with '-s' argument."
            ),
            "-a" -> Seq(
                "Prints details statistics about each cache.",
                "By default only aggregated summary is printed."
            ),
            "-p=<page size>" -> Seq(
                "Number of object to fetch from cache at once.",
                "Valid range from 1 to 100.",
                "By default page size is 25."
            )
        ),
        examples = Seq(
            "cache" ->
                "Prints summary statistics about all caches.",
            "cache -i" ->
                "Prints cache statistics for interactively selected node.",
            "cache -id8=12345678 -s=hi -r"  -> Seq(
                "Prints summary statistics about caches from node with specified id8",
                "sorted by number of hits in reverse order."
            ),
            "cache -id8=@n0 -s=hi -r"  -> Seq(
                "Prints summary statistics about caches from node with id8 taken from 'n0' memory variable.",
                "sorted by number of hits in reverse order."
            ),
            "cache -c=@c0 -a"  -> Seq(
                "Prints detailed statistics about cache with name taken from 'c0' memory variable."
            ),
            "cache -s=hi -r -a" ->
                "Prints detailed statistics about all caches sorted by number of hits in reverse order.",
            "cache -clear" -> "Clears interactively selected cache.",
            "cache -clear -c=cache" -> "Clears cache with name 'cache'.",
            "cache -clear -c=@c0" -> "Clears cache with name taken from 'c0' memory variable.",
            "cache -scan" -> "Prints list entries from interactively selected cache.",
            "cache -scan -c=cache" -> "List entries from cache with name 'cache' from all nodes with this cache.",
            "cache -scan -c=@c0 -p=50" -> ("Prints list entries from cache with name taken from 'c0' memory variable" +
                " with page of 50 items from all nodes with this cache."),
            "cache -scan -c=cache -id8=12345678" -> "Prints list entries from cache with name 'cache' and node '12345678' ID8.",
            "cache -swap" -> "Swaps entries in interactively selected cache.",
            "cache -swap -c=cache" -> "Swaps entries in cache with name 'cache'.",
            "cache -swap -c=@c0" -> "Swaps entries in cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.cache, cmd.cache)
    )

    /** Default cache name to show on screen. */
    private final val DFLT_CACHE_NAME = escapeName(null)
    
    /** Default cache key. */
    protected val DFLT_CACHE_KEY = DFLT_CACHE_NAME + "-" + UUID.randomUUID().toString

    /** Singleton command */
    private val cmd = new VisorCacheCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromCinfo2Visor(vs: VisorTag): VisorCacheCommand = cmd

    /**
     * Show table of cache configuration information.
     *
     * @param title Specified title for table.
     * @param cfg Config to show information.
     */
    private[commands] def showCacheConfiguration(title: String, cfg: VisorCacheConfiguration) {
        val affinityCfg = cfg.affinityConfiguration()
        val nearCfg = cfg.nearConfiguration()
        val preloadCfg = cfg.preloadConfiguration()
        val evictCfg = cfg.evictConfiguration()
        val defaultCfg = cfg.defaultConfiguration()
        val storeCfg = cfg.storeConfiguration()
        val queryCfg = cfg.queryConfiguration()

        val cacheT = VisorTextTable()

        cacheT #= ("Name", "Value")

        cacheT += ("Mode", cfg.mode)
        cacheT += ("Atomicity Mode", safe(cfg.atomicityMode))
        cacheT += ("Atomic Write Ordering Mode", safe(cfg.atomicWriteOrderMode))
        cacheT += ("Statistic Enabled", bool2Str(cfg.statisticsEnabled()))
        cacheT += ("Management Enabled", bool2Str(cfg.managementEnabled()))

        cacheT += ("Time To Live", defaultCfg.timeToLive())
        cacheT += ("Time To Live Eager Flag", cfg.eagerTtl)

        cacheT += ("Write Synchronization Mode", safe(cfg.writeSynchronizationMode))
        cacheT += ("Swap Enabled", bool2Str(cfg.swapEnabled()))
        cacheT += ("Invalidate", bool2Str(cfg.invalidate()))
        cacheT += ("Start Size", cfg.startSize())

        cacheT += ("Transaction Manager Lookup", safe(cfg.transactionManagerLookupClassName()))

        cacheT += ("Affinity Function", safe(affinityCfg.function()))
        cacheT += ("Affinity Backups", affinityCfg.partitionedBackups())
        cacheT += ("Affinity Partitions", safe(affinityCfg.partitions()))
        cacheT += ("Affinity Exclude Neighbors", safe(affinityCfg.excludeNeighbors()))
        cacheT += ("Affinity Mapper", safe(affinityCfg.mapper()))

        cacheT += ("Preload Mode", preloadCfg.mode())
        cacheT += ("Preload Batch Size", preloadCfg.batchSize())
        cacheT += ("Preload Thread Pool size", preloadCfg.threadPoolSize())
        cacheT += ("Preload Timeout", preloadCfg.timeout())
        cacheT += ("Preloading Delay", preloadCfg.partitionedDelay())
        cacheT += ("Time Between Preload Messages", preloadCfg.throttle())

        cacheT += ("Eviction Policy Enabled", bool2Str(evictCfg.policy() != null))
        cacheT += ("Eviction Policy", safe(evictCfg.policy()))
        cacheT += ("Eviction Policy Max Size", safe(evictCfg.policyMaxSize()))
        cacheT += ("Eviction Filter", safe(evictCfg.filter()))
        cacheT += ("Eviction Key Buffer Size", evictCfg.synchronizedKeyBufferSize())
        cacheT += ("Eviction Synchronized", bool2Str(evictCfg.evictSynchronized()))
        cacheT += ("Eviction Overflow Ratio", evictCfg.maxOverflowRatio())
        cacheT += ("Synchronous Eviction Timeout", evictCfg.synchronizedTimeout())
        cacheT += ("Synchronous Eviction Concurrency Level", evictCfg.synchronizedConcurrencyLevel())

        cacheT += ("Distribution Mode", cfg.distributionMode())

        cacheT += ("Near Start Size", nearCfg.nearStartSize())
        cacheT += ("Near Eviction Policy", safe(nearCfg.nearEvictPolicy()))
        cacheT += ("Near Eviction Enabled", bool2Str(nearCfg.nearEnabled()))
        cacheT += ("Near Eviction Synchronized", bool2Str(evictCfg.nearSynchronized()))
        cacheT += ("Near Eviction Policy Max Size", safe(nearCfg.nearEvictMaxSize()))

        cacheT += ("Default Lock Timeout", defaultCfg.txLockTimeout())
        cacheT += ("Default Query Timeout", defaultCfg.queryTimeout())
        cacheT += ("Query Indexing Enabled", bool2Str(cfg.queryIndexEnabled()))
        cacheT += ("Query Iterators Number", cfg.maxQueryIteratorCount())
        cacheT += ("Metadata type count", cfg.typeMeta().size())
        cacheT += ("Indexing SPI Name", safe(cfg.indexingSpiName()))
        cacheT += ("Cache Interceptor", safe(cfg.interceptor()))

        cacheT += ("Store Enabled", bool2Str(storeCfg.enabled()))
        cacheT += ("Store Class", safe(storeCfg.store()))
        cacheT += ("Store Factory Class", storeCfg.storeFactory())
        cacheT += ("Store Read Through", bool2Str(storeCfg.readThrough()))
        cacheT += ("Store Write Through", bool2Str(storeCfg.writeThrough()))

        cacheT += ("Write-Behind Enabled", bool2Str(storeCfg.enabled()))
        cacheT += ("Write-Behind Flush Size", storeCfg.flushSize())
        cacheT += ("Write-Behind Frequency", storeCfg.flushFrequency())
        cacheT += ("Write-Behind Flush Threads Count", storeCfg.flushThreadCount())
        cacheT += ("Write-Behind Batch Size", storeCfg.batchSize())

        cacheT += ("Concurrent Asynchronous Operations Number", cfg.maxConcurrentAsyncOperations())
        cacheT += ("Memory Mode", cfg.memoryMode())
        cacheT += ("Off-Heap Size", cfg.offsetHeapMaxMemory() match {
            case 0 => "UNLIMITED"
            case size if size < 0 => NA
            case size => size
        })

        cacheT += ("Loader Factory Class Name", safe(cfg.loaderFactory()))
        cacheT += ("Writer Factory Class Name", safe(cfg.writerFactory()))
        cacheT += ("Expiry Policy Factory Class Name", safe(cfg.expiryPolicyFactory()))

        if (queryCfg != null) {
            cacheT +=("Query Type Resolver", safe(queryCfg.typeResolver()))
            cacheT +=("Query Indexing Primitive Key", bool2Str(queryCfg.indexPrimitiveKey()))
            cacheT +=("Query Indexing Primitive Value", bool2Str(queryCfg.indexPrimitiveValue()))
            cacheT +=("Query Fixed Typing", bool2Str(queryCfg.indexFixedTyping()))
            cacheT +=("Query Escaped Names", bool2Str(queryCfg.escapeAll()))
        }
        else
            cacheT += ("Query Configuration", NA)

        println(title)

        cacheT.render()
    }
}
