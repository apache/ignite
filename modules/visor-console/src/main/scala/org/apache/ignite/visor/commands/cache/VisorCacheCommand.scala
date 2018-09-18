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

import java.util.{Collections, UUID, Collection => JavaCollection, List => JavaList}

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.typedef.X
import org.apache.ignite.internal.visor.cache._
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._
import org.jetbrains.annotations._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}

/**
 * ==Overview==
 * Visor 'cache' command implementation.
 *
 * ==Help==
 * {{{
 * +-------------------------------------------------------------------------------------------+
 * | cache            | Prints statistics about caches from specified node on the entire grid. |
 * |                  | Output sorting can be specified in arguments.                          |
 * |                  |                                                                        |
 * |                  | Output abbreviations:                                                  |
 * |                  |     #   Number of nodes.                                               |
 * |                  |     H/h Number of cache hits.                                          |
 * |                  |     M/m Number of cache misses.                                        |
 * |                  |     R/r Number of cache reads.                                         |
 * |                  |     W/w Number of cache writes.                                        |
 * +-------------------------------------------------------------------------------------------+
 * | cache -clear     | Clears all entries from cache on all nodes.                            |
 * +-------------------------------------------------------------------------------------------+
 * | cache -scan      | List all entries in cache with specified name.                         |
 * +-------------------------------------------------------------------------------------------+
 * | cache -stop      | Stop cache with specified name.                                        |
 * +-------------------------------------------------------------------------------------------+
 * | cache -reset     | Reset metrics for cache with specified name.                           |
 * +-------------------------------------------------------------------------------------------+
 * | cache -rebalance | Re-balance partitions for cache with specified name.                   |
 * +-------------------------------------------------------------------------------------------+
 * | cache -slp       | Show list of lost partitions for specified cache.                      |
 * +-------------------------------------------------------------------------------------------+
 * | cache -rlp       | Reset lost partitions for specified cache.                             |
 * +-------------------------------------------------------------------------------------------+
 *
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache {-system}
 *     cache -i {-system}
 *     cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr|cn} {-a} {-r} {-system}
 *     cache -clear {-c=<cache-name>}
 *     cache -scan -c=<cache-name> {-near} {-id=<node-id>|id8=<node-id8>} {-p=<page size>} {-system}
 *     cache -stop -c=<cache-name>
 *     cache -reset -c=<cache-name>
 *     cache -rebalance -c=<cache-name>
 *     cache -slp -c=<cache-name>
 *     cache -rlp -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8=<node-id>
 *         ID8 of the node to get cache statistics from.
 *         Note that either '-id8' or '-id' should be specified.
 *         You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.
 *         To specify oldest node on the same host as visor use variable '@nl'.
 *         To specify oldest node on other hosts that are not running visor use variable '@nr'.
 *         If neither is specified statistics will be gathered from all nodes.
 *     -id=<node-id>
 *         Full ID of the node to get cache statistics from.
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
 *     -system
 *         Enable showing of information about system caches.
 *     -clear
 *         Clears cache.
 *     -scan
 *         Prints list of all entries from cache.
 *     -near
 *         Prints list of all entries from near cache of cache.
 *     -stop
 *          Stop cache with specified name.
 *     -reset
 *          Reset metrics for cache with specified name.
 *     -rebalance
 *          Re-balance partitions for cache with specified name.
 *     -slp
 *          Show list of lost partitions for specified cache.
 *     -rlp
 *          Reset lost partitions for specified cache.
 *     -p=<page size>
 *         Number of object to fetch from cache at once.
 *         Valid range from 1 to 100.
 *         By default page size is 25.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cache
 *         Prints summary statistics about all no system caches.
 *     cache -system
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
 *     cache -scan -c=cache -near -id8=12345678
 *         Prints list entries from near cache of cache with name 'cache' and node '12345678' ID8.
 *     cache -stop -c=cache
 *         Stops cache with name 'cache'.
 *     cache -reset -c=cache
 *         Reset metrics for cache with name 'cache'.
 *     cache -rebalance -c=cache
 *         Re-balance partitions for cache with name 'cache'.
 *     cache -slp -c=cache
 *         Show list of lost partitions for cache with name 'cache'.
 *     cache -rlp -c=cache
 *         Reset lost partitions for cache with name 'cache'.
 *
 * }}}
 */
class VisorCacheCommand extends VisorConsoleCommand {
    @impl protected val name: String = "cache"

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
     * <ex>cache -scan -c=cache -near -id8=12345678</ex>
     *     Prints list entries from near cache of cache with name 'cache' and node '12345678' ID8.
     * <br>
     * <ex>cache -stop -c=@c0</ex>
     *     Stop cache with name taken from 'c0' memory variable.
     * <br>
     * <ex>cache -reset -c=@c0</ex>
     *     Reset metrics for cache with name taken from 'c0' memory variable.
     *
     * @param args Command arguments.
     */
    def cache(args: String) {
        if (checkConnected() && checkActiveState()) {
            var argLst = parseArgs(args)

            if (hasArgFlag("i", argLst)) {
                askForNode("Select node from:") match {
                    case Some(nid) => ask("Detailed statistics (y/n) [n]: ", "n") match {
                        case "n" | "N" => nl(); cache("-id=" + nid); return;
                        case "y" | "Y" => nl(); cache("-a -id=" + nid); return;
                        case x => nl(); warn("Invalid answer: " + x); return;
                    }
                    case None => return
                }

                return
            }

            val node = parseNode(argLst) match {
                case Left(msg) =>
                    scold(msg)

                    return

                case Right(n) => n match {
                    case None if hasArgName("scan", argLst) && hasArgName("near", argLst) =>
                        askForNode("Select node from:") match {
                            case None => return

                            case nidOpt => nidOpt.map(ignite.cluster.node(_))
                        }

                    case _ => n
                }
            }

            val showSystem = hasArgFlag("system", argLst)

            var cacheName = argValue("c", argLst) match {
                case Some(dfltName) if dfltName == DFLT_CACHE_KEY || dfltName == DFLT_CACHE_NAME =>
                    argLst = argLst.filter(_._1 != "c") ++ Seq("c" -> null)

                    Some(null)

                case cn => cn
            }

            /** Check that argument list has flag from list. */
            def hasArgFlagIn(flags: String *) = {
                flags.exists(hasArgFlag(_, argLst))
            }

            // Get cache stats data from all nodes.
            val aggrData = cacheData(node, cacheName, showSystem)

            if (hasArgFlagIn("clear", "scan", "stop", "reset", "rebalance", "slp", "rlp")) {
                if (cacheName.isEmpty)
                    askForCache("Select cache from:", node, showSystem
                        && !hasArgFlagIn("clear", "stop", "reset", "rebalance"), aggrData) match {
                        case Some(name) =>
                            argLst = argLst ++ Seq("c" -> name)

                            cacheName = Some(name)

                        case None => return
                    }

                cacheName.foreach(name => {
                    aggrData.find(cache => F.eq(cache.getName, name)) match {
                        case Some(cache) =>
                            if (!cache.isSystem) {
                                if (hasArgFlag("scan", argLst))
                                    VisorCacheScanCommand().scan(argLst, node)
                                else if (hasArgFlag("clear", argLst))
                                    VisorCacheClearCommand().clear(argLst, node)
                                else if (hasArgFlag("stop", argLst))
                                    VisorCacheStopCommand().stop(argLst, node)
                                else if (hasArgFlag("reset", argLst))
                                    VisorCacheResetCommand().reset(argLst, node)
                                else if (hasArgFlag("rebalance", argLst))
                                    VisorCacheRebalanceCommand().rebalance(argLst, node)
                                else if (hasArgFlag("slp", argLst))
                                    VisorCacheLostPartitionsCommand().showLostPartitions(argLst, node)
                                else if (hasArgFlag("rlp", argLst))
                                    VisorCacheResetLostPartitionsCommand().resetLostPartitions(argLst, node)
                            }
                            else {
                                if (hasArgFlag("scan", argLst))
                                    warn("Scan of system cache is not allowed: " + name)
                                else if (hasArgFlag("clear", argLst))
                                    warn("Clearing of system cache is not allowed: " + name)
                                else if (hasArgFlag("stop", argLst))
                                    warn("Stopping of system cache is not allowed: " + name)
                                else if (hasArgFlag("reset", argLst))
                                    warn("Reset metrics of system cache is not allowed: " + name)
                                else if (hasArgFlag("rebalance", argLst))
                                    warn("Re-balance partitions of system cache is not allowed: " + name)
                            }
                        case None =>
                            warn("Cache with specified name not found: " + name)
                    }
                })

                return
            }

            val all = hasArgFlag("a", argLst)

            val sortType = argValue("s", argLst)
            val reversed = hasArgName("r", argLst)

            if (sortType.isDefined && !isValidSortType(sortType.get)) {
                scold("Invalid '-s' argument in: " + args)

                return
            }

            if (aggrData.isEmpty) {
                scold("No caches found.")

                return
            }

            node match {
                case Some(n) =>
                    println("ID8=" + nid8(n) + ", time of the snapshot: " + formatDateTime(System.currentTimeMillis))
                case None =>
                    println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))
            }

            val sumT = VisorTextTable()

            sumT #= ("Name(@)", "Mode", "Nodes", "Total entries (Heap / Off-heap)", "Primary entries (Heap / Off-heap)", "Hits", "Misses", "Reads", "Writes")

            sortAggregatedData(aggrData, sortType.getOrElse("cn"), reversed).foreach(
                ad => {
                    // Add cache host as visor variable.
                    registerCacheName(ad.getName)

                    sumT += (
                        mkCacheName(ad.getName),
                        ad.getMode,
                        ad.getNodes.size(),
                        (ad.getTotalHeapSize + ad.getTotalOffHeapSize) + " (" + ad.getTotalHeapSize + " / " + ad.getTotalOffHeapSize + ")",
                        (
                            "min: " + (ad.getMinimumHeapSize + ad.getMinimumOffHeapPrimarySize) +
                                " (" + ad.getMinimumHeapSize + " / " + ad.getMinimumOffHeapPrimarySize + ")",
                            "avg: " + formatDouble(ad.getAverageHeapSize + ad.getAverageOffHeapPrimarySize) +
                                " (" + formatDouble(ad.getAverageHeapSize) + " / " + formatDouble(ad.getAverageOffHeapPrimarySize) + ")",
                            "max: " + (ad.getMaximumHeapSize + ad.getMaximumOffHeapPrimarySize) +
                                " (" + ad.getMaximumHeapSize + " / " + ad.getMaximumOffHeapPrimarySize + ")"
                            ),
                        (
                            "min: " + ad.getMinimumHits,
                            "avg: " + formatDouble(ad.getAverageHits),
                            "max: " + ad.getMaximumHits
                            ),
                        (
                            "min: " + ad.getMinimumMisses,
                            "avg: " + formatDouble(ad.getAverageMisses),
                            "max: " + ad.getMaximumMisses
                            ),
                        (
                            "min: " + ad.getMinimumReads,
                            "avg: " + formatDouble(ad.getAverageReads),
                            "max: " + ad.getMaximumReads
                            ),
                        (
                            "min: " + ad.getMinimumWrites,
                            "avg: " + formatDouble(ad.getAverageWrites),
                            "max: " + ad.getMaximumWrites
                            )
                        )
                }
            )

            sumT.render()

            if (all) {
                val sorted = aggrData.sortWith((k1, k2) => {
                    if (k1.getName == null)
                        true
                    else if (k2.getName == null)
                        false
                    else k1.getName.compareTo(k2.getName) < 0
                })

                val gCfg = node.map(config).collect {
                    case cfg if cfg != null => cfg
                }

                sorted.foreach(ad => {
                    val cacheNameVar = mkCacheName(ad.getName)

                    println("\nCache '" + cacheNameVar + "':")

                    val m = ad.getMetrics

                    val csT = VisorTextTable()

                    csT += ("Name(@)", cacheNameVar)
                    csT += ("Total entries (Heap / Off-heap)", (ad.getTotalHeapSize + ad.getTotalOffHeapSize) +
                        " (" + ad.getTotalHeapSize + " / " + ad.getTotalOffHeapSize + ")")
                    csT += ("Nodes", m.size())
                    csT += ("Total size Min/Avg/Max", (ad.getMinimumHeapSize + ad.getMinimumOffHeapPrimarySize) + " / " +
                        formatDouble(ad.getAverageHeapSize + ad.getAverageOffHeapPrimarySize) + " / " +
                        (ad.getMaximumHeapSize + ad.getMaximumOffHeapPrimarySize))
                    csT += ("  Heap size Min/Avg/Max", ad.getMinimumHeapSize + " / " +
                        formatDouble(ad.getAverageHeapSize) + " / " + ad.getMaximumHeapSize)
                    csT += ("  Off-heap size Min/Avg/Max", ad.getMinimumOffHeapPrimarySize + " / " +
                        formatDouble(ad.getAverageOffHeapPrimarySize) + " / " + ad.getMaximumOffHeapPrimarySize)

                    val ciT = VisorTextTable()

                    ciT #= ("Node ID8(@), IP", "CPUs", "Heap Used", "CPU Load", "Up Time", "Size (Primary / Backup)", "Hi/Mi/Rd/Wr")

                    sortData(m.toMap, sortType.getOrElse("hi"), reversed).foreach { case (nid, cm) =>
                        val nm = ignite.cluster.node(nid).metrics()

                        ciT += (
                            nodeId8Addr(nid),
                            nm.getTotalCpus,
                            formatDouble(100d * nm.getHeapMemoryUsed / nm.getHeapMemoryMaximum) + " %",

                            formatDouble(nm.getCurrentCpuLoad * 100d) + " %",
                            X.timeSpan2HMSM(nm.getUpTime),
                            (
                                "Total: " + (cm.getHeapEntriesCount + cm.getOffHeapEntriesCount) +
                                    " (" + (cm.getHeapEntriesCount + cm.getOffHeapPrimaryEntriesCount) + " / " + cm.getOffHeapBackupEntriesCount + ")",
                                "  Heap: " + cm.getHeapEntriesCount + " (" + cm.getHeapEntriesCount + " / " + NA + ")",
                                "  Off-Heap: " + cm.getOffHeapEntriesCount +
                                    " (" + cm.getOffHeapPrimaryEntriesCount + " / " + cm.getOffHeapBackupEntriesCount + ")",
                                "  Off-Heap Memory: " + (if (cm.getOffHeapPrimaryEntriesCount == 0) "0"
                                    else if (cm.getOffHeapAllocatedSize > 0) formatMemory(cm.getOffHeapAllocatedSize)
                                    else NA)
                            ),
                            (
                                "Hi: " + cm.getHits,
                                "Mi: " + cm.getMisses,
                                "Rd: " + cm.getReads,
                                "Wr: " + cm.getWrites
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
                    println("  Minimum execution time: " + X.timeSpan2HMSM(ad.getMinimumQueryTime))
                    println("  Maximum execution time: " + X.timeSpan2HMSM(ad.getMaximumQueryTime))
                    println("  Average execution time: " + X.timeSpan2HMSM(ad.getAverageQueryTime.toLong))
                    println("  Total number of executions: " + ad.getQueryExecutions)
                    println("  Total number of failures:   " + ad.getQueryFailures)

                    gCfg.foreach(ccfgs => ccfgs.find(ccfg => F.eq(ccfg.getName, ad.getName))
                        .foreach(ccfg => {
                            nl()

                            printCacheConfiguration("Cache configuration:", ccfg)
                    }))
                })
            }
            else
                println("\nUse \"-a\" flag to see detailed statistics.")
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
            val v = mfindHead(DFLT_CACHE_KEY)

            DFLT_CACHE_NAME + (if (v.isDefined) "(@" + v.get._1 + ')' else "")
        }
        else {
            val v = mfindHead(s)

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
     * @param node Option of node for cache names extracting. All nodes if `None`.
     * @param systemCaches Allow selection of system caches.
     * @return Caches metrics data.
     */
    private def cacheData(node: Option[ClusterNode], name: Option[String], systemCaches: Boolean = false):
        List[VisorCacheAggregatedMetrics] = {
        assert(node != null)

        try {
            val caches: JavaList[String] = name.fold(Collections.emptyList[String]())(Collections.singletonList)

            val arg = new VisorCacheMetricsCollectorTaskArg(systemCaches, caches)

            node match {
                case Some(n) => executeOne(n.id(), classOf[VisorCacheMetricsCollectorTask], arg).toList
                case None => executeMulti(classOf[VisorCacheMetricsCollectorTask], arg).toList
            }
        }
        catch {
            case e: IgniteException => Nil
        }
    }

    /**
     * Gets configuration of grid from specified node for collecting of node cache's configuration.
     *
     * @param node Specified node.
     * @return Cache configurations for specified node.
     */
    private def config(node: ClusterNode): JavaCollection[VisorCacheConfiguration] = {
        try {
            cacheConfigurations(node.id())
        }
        catch {
            case e: IgniteException =>
                scold(e)

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
            case "hi" => data.toSeq.sortBy(_._2.getHits)
            case "mi" => data.toSeq.sortBy(_._2.getMisses)
            case "rd" => data.toSeq.sortBy(_._2.getReads)
            case "wr" => data.toSeq.sortBy(_._2.getWrites)
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
            case "hi" => data.toList.sortBy(_.getAverageHits)
            case "mi" => data.toList.sortBy(_.getAverageMisses)
            case "rd" => data.toList.sortBy(_.getAverageReads)
            case "wr" => data.toList.sortBy(_.getAverageWrites)
            case "cn" => data.toList.sortWith((x, y) =>
                x.getName == null || (y.getName != null && x.getName.toLowerCase < y.getName.toLowerCase))

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
     * @param node Option of node for cache names extracting. All nodes if `None`.
     * @param showSystem Allow selection of system caches.
     * @return `Option` for ID of selected cache.
     */
    def askForCache(title: String, node: Option[ClusterNode], showSystem: Boolean = false,
        aggrData: Seq[VisorCacheAggregatedMetrics]): Option[String] = {
        assert(title != null)

        if (aggrData.isEmpty) {
            scold("No caches found.")

            return None
        }

        val sortedAggrData = sortAggregatedData(aggrData, "cn", false)

        println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

        val sumT = VisorTextTable()

        sumT #= ("#", "Name(@)", "Mode", "Size (Heap / Off-heap)")

        sortedAggrData.indices.foreach(i => {
            val ad = sortedAggrData(i)

            // Add cache host as visor variable.
            registerCacheName(ad.getName)

            sumT += (
                i,
                mkCacheName(ad.getName),
                ad.getMode,
                (
                    "min: " + (ad.getMinimumHeapSize + ad.getMinimumOffHeapPrimarySize) +
                        " (" + ad.getMinimumHeapSize + " / " + ad.getMinimumOffHeapPrimarySize + ")",
                    "avg: " + formatDouble(ad.getAverageHeapSize + ad.getAverageOffHeapPrimarySize) +
                        " (" + formatDouble(ad.getAverageHeapSize) + " / " + formatDouble(ad.getAverageOffHeapPrimarySize) + ")",
                    "max: " + (ad.getMaximumHeapSize + ad.getMaximumOffHeapPrimarySize) +
                        " (" + ad.getMaximumHeapSize + " / " + ad.getMaximumOffHeapPrimarySize + ")"
                ))
        })

        sumT.render()

        val a = ask("\nChoose cache number ('c' to cancel) [c]: ", "0")

        if (a.toLowerCase == "c")
            None
        else {
            try
                Some(sortedAggrData(a.toInt).getName)
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
    /** Singleton command */
    private val cmd = new VisorCacheCommand

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
            "Prints or clear list lost partitions from cache."
        ),
        spec = Seq(
            "cache",
            "cache -i",
            "cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr} {-a} {-r}",
            "cache -clear {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}",
            "cache -scan -c=<cache-name> {-near} {-id=<node-id>|id8=<node-id8>} {-p=<page size>}",
            "cache -stop -c=<cache-name>",
            "cache -reset -c=<cache-name>",
            "cache -rebalance -c=<cache-name>",
            "cache -slp -c=<cache-name>",
            "cache -rlp -c=<cache-name>"
  ),
        args = Seq(
            "-id8=<node-id>" -> Seq(
                "ID8 of the node to get cache statistics from.",
                "Note that either '-id8' or '-id' should be specified.",
                "You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.",
                "To specify oldest node on the same host as visor use variable '@nl'.",
                "To specify oldest node on other hosts that are not running visor use variable '@nr'.",
                "If neither is specified statistics will be gathered from all nodes."
            ),
            "-id=<node-id>" -> Seq(
                "Full ID of the node to get cache statistics from.",
                "Either '-id8' or '-id' can be specified.",
                "If neither is specified statistics will be gathered from all nodes."
            ),
            "-c=<cache-name>" -> Seq(
                "Name of the cache.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            ),
            "-clear" -> "Clears cache.",
            "-system" -> "Enable showing of information about system caches.",
            "-scan" -> "Prints list of all entries from cache.",
            "-near" -> "Prints list of all entries from near cache of cache.",
            "-stop" -> "Stop cache with specified name.",
            "-reset" -> "Reset metrics of cache with specified name.",
            "-slp" -> "Show list of lost partitions for specified cache.",
            "-rlp" -> "Reset lost partitions for specified cache.",
            "-rebalance" -> "Re-balance partitions for cache with specified name.",
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
                "Prints summary statistics about all non-system caches.",
            "cache -system" ->
                "Prints summary statistics about all caches including system cache.",
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
            "cache -scan -near -c=cache -id8=12345678" ->
                "Prints list entries from near cache of cache with name 'cache' and node '12345678' ID8.",
            "cache -stop -c=@c0" -> "Stop cache with name taken from 'c0' memory variable.",
            "cache -reset -c=@c0" -> "Reset metrics for cache with name taken from 'c0' memory variable.",
            "cache -rebalance -c=cache" -> "Re-balance partitions for cache with name 'cache'.",
            "cache -slp -c=@c0" -> "Show list of lost partitions for cache with name taken from 'c0' memory variable.",
            "cache -rlp -c=@c0" -> "Reset lost partitions for cache with name taken from 'c0' memory variable."
        ),
        emptyArgs = cmd.cache,
        withArgs = cmd.cache
    )

    /** Default cache name to show on screen. */
    private final val DFLT_CACHE_NAME = escapeName(null)

    /** Default cache key. */
    protected val DFLT_CACHE_KEY = DFLT_CACHE_NAME + "-" + UUID.randomUUID().toString

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
    private[commands] def printCacheConfiguration(title: String, cfg: VisorCacheConfiguration) {
        val affinityCfg = cfg.getAffinityConfiguration
        val nearCfg = cfg.getNearConfiguration
        val rebalanceCfg = cfg.getRebalanceConfiguration
        val evictCfg = cfg.getEvictionConfiguration
        val storeCfg = cfg.getStoreConfiguration
        val queryCfg = cfg.getQueryConfiguration

        val cacheT = VisorTextTable()

        cacheT #= ("Name", "Value")

        cacheT += ("Group", cfg.getGroupName)
        cacheT += ("Dynamic Deployment ID", cfg.getDynamicDeploymentId)
        cacheT += ("System", bool2Str(cfg.isSystem))

        cacheT += ("Mode", cfg.getMode)
        cacheT += ("Atomicity Mode", safe(cfg.getAtomicityMode))
        cacheT += ("Statistic Enabled", bool2Str(cfg.isStatisticsEnabled))
        cacheT += ("Management Enabled", bool2Str(cfg.isManagementEnabled))

        cacheT += ("On-heap cache enabled", bool2Str(cfg.isOnheapCacheEnabled))
        cacheT += ("Partition Loss Policy", cfg.getPartitionLossPolicy)
        cacheT += ("Query Parallelism", cfg.getQueryParallelism)
        cacheT += ("Copy On Read", bool2Str(cfg.isCopyOnRead))
        cacheT += ("Listener Configurations", cfg.getListenerConfigurations)
        cacheT += ("Load Previous Value", bool2Str(cfg.isLoadPreviousValue))
        cacheT += ("Memory Policy Name", cfg.getMemoryPolicyName)
        cacheT += ("Node Filter", cfg.getNodeFilter)
        cacheT += ("Read From Backup", bool2Str(cfg.isReadFromBackup))
        cacheT += ("Topology Validator", cfg.getTopologyValidator)

        cacheT += ("Time To Live Eager Flag", cfg.isEagerTtl)

        cacheT += ("Write Synchronization Mode", safe(cfg.getWriteSynchronizationMode))
        cacheT += ("Invalidate", bool2Str(cfg.isInvalidate))

        cacheT += ("Affinity Function", safe(affinityCfg.getFunction))
        cacheT += ("Affinity Backups", affinityCfg.getPartitionedBackups)
        cacheT += ("Affinity Partitions", safe(affinityCfg.getPartitions))
        cacheT += ("Affinity Exclude Neighbors", safe(affinityCfg.isExcludeNeighbors))
        cacheT += ("Affinity Mapper", safe(affinityCfg.getMapper))

        cacheT += ("Rebalance Mode", rebalanceCfg.getMode)
        cacheT += ("Rebalance Batch Size", rebalanceCfg.getBatchSize)
        cacheT += ("Rebalance Timeout", rebalanceCfg.getTimeout)
        cacheT += ("Rebalance Delay", rebalanceCfg.getPartitionedDelay)
        cacheT += ("Time Between Rebalance Messages", rebalanceCfg.getThrottle)
        cacheT += ("Rebalance Batches Count", rebalanceCfg.getBatchesPrefetchCnt)
        cacheT += ("Rebalance Cache Order", rebalanceCfg.getRebalanceOrder)

        cacheT += ("Eviction Policy Enabled", bool2Str(evictCfg.getPolicy != null))
        cacheT += ("Eviction Policy Factory", safe(evictCfg.getPolicy))
        cacheT += ("Eviction Policy Max Size", safe(evictCfg.getPolicyMaxSize))
        cacheT += ("Eviction Filter", safe(evictCfg.getFilter))

        cacheT += ("Near Cache Enabled", bool2Str(nearCfg.isNearEnabled))
        cacheT += ("Near Start Size", nearCfg.getNearStartSize)
        cacheT += ("Near Eviction Policy Factory", safe(nearCfg.getNearEvictPolicy))
        cacheT += ("Near Eviction Policy Max Size", safe(nearCfg.getNearEvictMaxSize))

        cacheT += ("Default Lock Timeout", cfg.getDefaultLockTimeout)
        cacheT += ("Metadata type count", cfg.getJdbcTypes.size())
        cacheT += ("Cache Interceptor", safe(cfg.getInterceptor))

        cacheT += ("Store Enabled", bool2Str(storeCfg.isEnabled))
        cacheT += ("Store Class", safe(storeCfg.getStore))
        cacheT += ("Store Factory Class", storeCfg.getStoreFactory)
        cacheT += ("Store Keep Binary", storeCfg.isStoreKeepBinary)
        cacheT += ("Store Read Through", bool2Str(storeCfg.isReadThrough))
        cacheT += ("Store Write Through", bool2Str(storeCfg.isWriteThrough))
        cacheT += ("Store Write Coalescing", bool2Str(storeCfg.getWriteBehindCoalescing))

        cacheT += ("Write-Behind Enabled", bool2Str(storeCfg.isWriteBehindEnabled))
        cacheT += ("Write-Behind Flush Size", storeCfg.getFlushSize)
        cacheT += ("Write-Behind Frequency", storeCfg.getFlushFrequency)
        cacheT += ("Write-Behind Flush Threads Count", storeCfg.getFlushThreadCount)
        cacheT += ("Write-Behind Batch Size", storeCfg.getBatchSize)

        cacheT += ("Concurrent Asynchronous Operations Number", cfg.getMaxConcurrentAsyncOperations)

        cacheT += ("Loader Factory Class Name", safe(cfg.getLoaderFactory))
        cacheT += ("Writer Factory Class Name", safe(cfg.getWriterFactory))
        cacheT += ("Expiry Policy Factory Class Name", safe(cfg.getExpiryPolicyFactory))

        cacheT +=("Query Execution Time Threshold", queryCfg.getLongQueryWarningTimeout)
        cacheT +=("Query Escaped Names", bool2Str(queryCfg.isSqlEscapeAll))
        cacheT +=("Query Schema Name", queryCfg.getSqlSchema)
        cacheT +=("Query Indexed Types", queryCfg.getIndexedTypes)
        cacheT +=("Maximum payload size for offheap indexes", cfg.getSqlIndexMaxInlineSize)
        cacheT +=("Query Metrics History Size", cfg.getQueryDetailMetricsSize)

        val sqlFxs = queryCfg.getSqlFunctionClasses

        val hasSqlFxs = sqlFxs != null && sqlFxs.nonEmpty

        if (!hasSqlFxs)
            cacheT +=("Query SQL functions", NA)

        val indexedTypes = queryCfg.getIndexedTypes

        val hasIndexedTypes = indexedTypes != null && indexedTypes.nonEmpty

        if (!hasIndexedTypes)
            cacheT +=("Query Indexed Types", NA)

        println(title)

        cacheT.render()

        if (hasSqlFxs) {
            println("\nQuery SQL functions:")

            val sqlFxsT = VisorTextTable()

            sqlFxsT #= "Function Class Name"

            sqlFxs.foreach(s => sqlFxsT += s)

            sqlFxsT.render()
        }

        if (hasIndexedTypes) {
            println("\nQuery Indexed Types:")

            val indexedTypesT = VisorTextTable()

            indexedTypesT #= ("Key Class Name", "Value Class Name")

            indexedTypes.grouped(2).foreach(types => indexedTypesT += (types(0), types(1)))

            indexedTypesT.render()
        }
    }
}
