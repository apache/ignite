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

import java.lang.{Boolean => JavaBoolean}
import java.util.{Collection => JavaCollection, Collections, UUID}

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.util.typedef._
import org.apache.ignite.internal.visor.cache._
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.lang.IgniteBiTuple
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.commands.common.VisorTextTable
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
 * | cache -stop    | Stop cache with specified name.                                        |
 * +-----------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache {-system}
 *     cache -i {-system}
 *     cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr|cn} {-a} {-r} {-system}
 *     cache -clear {-c=<cache-name>}
 *     cache -scan -c=<cache-name> {-id=<node-id>|id8=<node-id8>} {-p=<page size>} {-system}
 *     cache -swap {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}
 *     cache -stop -c=<cache-name>
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
 *          Clears cache.
 *     -scan
 *          Prints list of all entries from cache.
 *     -swap
 *          Swaps backup entries in cache.
 *     -stop
 *          Stop cache with specified name.
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
 *     cache -swap
 *         Swaps entries in interactively selected cache.
 *     cache -swap -c=cache
 *         Swaps entries in cache with name 'cache'.
 *     cache -swap -c=@c0
 *         Swaps entries in cache with name taken from 'c0' memory variable.
 *     cache -stop -c=cache
 *         Stops cache with name 'cache'.
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
     * <br>
     * <ex>cache -stop -c=@c0</ex>
     *     Stop cache with name taken from 'c0' memory variable.
     *
     * @param args Command arguments.
     */
    def cache(args: String) {
        if (!isConnected)
            adviseToConnect()
        else {
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

                case Right(n) => n
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

            if (hasArgFlagIn("clear", "swap", "scan", "stop")) {
                if (cacheName.isEmpty)
                    askForCache("Select cache from:", node, showSystem && !hasArgFlagIn("clear", "swap", "stop"), aggrData) match {
                        case Some(name) =>
                            argLst = argLst ++ Seq("c" -> name)

                            cacheName = Some(name)

                        case None => return
                    }

                cacheName.foreach(name => {
                    if (hasArgFlag("scan", argLst))
                        VisorCacheScanCommand().scan(argLst, node)
                    else {
                        if (aggrData.nonEmpty && !aggrData.exists(cache => safeEquals(cache.name(), name) && cache.system())) {
                            if (hasArgFlag("clear", argLst))
                                VisorCacheClearCommand().clear(argLst, node)
                            else if (hasArgFlag("swap", argLst))
                                VisorCacheSwapCommand().swap(argLst, node)
                            else if (hasArgFlag("stop", argLst))
                                VisorCacheStopCommand().stop(argLst, node)
                        }
                        else {
                            if (hasArgFlag("clear", argLst))
                                warn("Clearing of system cache is not allowed: " + name)
                            else if (hasArgFlag("swap", argLst))
                                warn("Backup swapping of system cache is not allowed: " + name)
                            else if (hasArgFlag("stop", argLst))
                                warn("Stopping of system cache is not allowed: " + name)
                        }
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

            sumT #= ("Name(@)", "Mode", "Nodes", "Entries (Heap / Off heap)", "Hits", "Misses", "Reads", "Writes")

            sortAggregatedData(aggrData, sortType.getOrElse("cn"), reversed).foreach(
                ad => {
                    // Add cache host as visor variable.
                    registerCacheName(ad.name())

                    sumT += (
                        mkCacheName(ad.name()),
                        ad.mode(),
                        ad.nodes.size(),
                        (
                            "min: " + (ad.minimumHeapSize() + ad.minimumOffHeapSize()) +
                                " (" + ad.minimumHeapSize() + " / " + ad.minimumOffHeapSize() + ")",
                            "avg: " + formatDouble(ad.averageHeapSize() + ad.averageOffHeapSize()) +
                                " (" + formatDouble(ad.averageHeapSize()) + " / " + formatDouble(ad.averageOffHeapSize()) + ")",
                            "max: " + (ad.maximumHeapSize() + ad.maximumOffHeapSize()) +
                                " (" + ad.maximumHeapSize() + " / " + ad.maximumOffHeapSize() + ")"
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
                    if (k1.name() == null)
                        true
                    else if (k2.name() == null)
                        false
                    else k1.name().compareTo(k2.name()) < 0
                })

                val gCfg = node.map(config).collect {
                    case cfg if cfg != null => cfg
                }

                sorted.foreach(ad => {
                    val cacheNameVar = mkCacheName(ad.name())

                    println("\nCache '" + cacheNameVar + "':")

                    val m = ad.metrics()

                    val csT = VisorTextTable()

                    csT += ("Name(@)", cacheNameVar)
                    csT += ("Nodes", m.size())
                    csT += ("Total size Min/Avg/Max", (ad.minimumHeapSize() + ad.minimumOffHeapSize()) + " / " +
                        formatDouble(ad.averageHeapSize() + ad.averageOffHeapSize()) + " / " +
                        (ad.maximumHeapSize() + ad.maximumOffHeapSize()))
                    csT += ("  Heap size Min/Avg/Max", ad.minimumHeapSize() + " / " +
                        formatDouble(ad.averageHeapSize()) + " / " + ad.maximumHeapSize())
                    csT += ("  Off heap size Min/Avg/Max", ad.minimumOffHeapSize() + " / " +
                        formatDouble(ad.averageOffHeapSize()) + " / " + ad.maximumOffHeapSize())

                    val ciT = VisorTextTable()

                    ciT #= ("Node ID8(@), IP", "CPUs", "Heap Used", "CPU Load", "Up Time", "Size", "Hi/Mi/Rd/Wr")

                    sortData(m.toMap, sortType.getOrElse("hi"), reversed).foreach { case (nid, cm) =>
                        val nm = ignite.cluster.node(nid).metrics()

                        ciT += (
                            nodeId8Addr(nid),
                            nm.getTotalCpus,
                            formatDouble(100d * nm.getHeapMemoryUsed / nm.getHeapMemoryMaximum) + " %",

                            formatDouble(nm.getCurrentCpuLoad * 100d) + " %",
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

                    gCfg.foreach(ccfgs => ccfgs.find(ccfg => safeEquals(ccfg.name(), ad.name()))
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
            val caches: JavaCollection[String] = name.fold(Collections.emptyList[String]())(Collections.singletonList)

            val arg = new IgniteBiTuple(JavaBoolean.valueOf(systemCaches), caches)

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
                x.name() == null || (y.name() != null && x.name().toLowerCase < y.name().toLowerCase))

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
        assert(visor.visor.isConnected)

        if (aggrData.isEmpty) {
            scold("No caches found.")

            return None
        }

        val sortedAggrData = sortAggregatedData(aggrData, "cn", false)

        println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

        val sumT = VisorTextTable()

        sumT #= ("#", "Name(@)", "Mode", "Size (Heap / Off heap)")

        sortedAggrData.indices.foreach(i => {
            val ad = sortedAggrData(i)

            // Add cache host as visor variable.
            registerCacheName(ad.name())

            sumT += (
                i,
                mkCacheName(ad.name()),
                ad.mode(),
                (
                    "min: " + (ad.minimumHeapSize() + ad.minimumOffHeapSize()) +
                        " (" + ad.minimumHeapSize() + " / " + ad.minimumOffHeapSize() + ")",
                    "avg: " + formatDouble(ad.averageHeapSize() + ad.averageOffHeapSize()) +
                        " (" + formatDouble(ad.averageHeapSize()) + " / " + formatDouble(ad.averageOffHeapSize()) + ")",
                    "max: " + (ad.maximumHeapSize() + ad.maximumOffHeapSize()) +
                        " (" + ad.maximumHeapSize() + " / " + ad.maximumOffHeapSize() + ")"
                ))
        })

        sumT.render()

        val a = ask("\nChoose cache number ('c' to cancel) [c]: ", "0")

        if (a.toLowerCase == "c")
            None
        else {
            try
                Some(sortedAggrData(a.toInt).name())
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
            "Swaps backup entries in cache."
        ),
        spec = Seq(
            "cache",
            "cache -i",
            "cache {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=hi|mi|rd|wr} {-a} {-r}",
            "cache -clear {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}",
            "cache -scan -c=<cache-name> {-id=<node-id>|id8=<node-id8>} {-p=<page size>}",
            "cache -swap {-c=<cache-name>} {-id=<node-id>|id8=<node-id8>}",
            "cache -stop -c=<cache-name>"
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
            "-clear" -> Seq(
                "Clears cache."
            ),
            "-system" -> Seq(
                "Enable showing of information about system caches."
            ),
            "-scan" -> Seq(
                "Prints list of all entries from cache."
            ),
            "-swap" -> Seq(
                "Swaps backup entries in cache."
            ),
            "-stop" -> Seq(
                "Stop cache with specified name"
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
            "cache -swap" -> "Swaps entries in interactively selected cache.",
            "cache -swap -c=cache" -> "Swaps entries in cache with name 'cache'.",
            "cache -swap -c=@c0" -> "Swaps entries in cache with name taken from 'c0' memory variable.",
            "cache -stop -c=@c0" -> "Stop cache with name taken from 'c0' memory variable."
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
        val affinityCfg = cfg.affinityConfiguration()
        val nearCfg = cfg.nearConfiguration()
        val rebalanceCfg = cfg.rebalanceConfiguration()
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

        cacheT += ("Time To Live Eager Flag", cfg.eagerTtl)

        cacheT += ("Write Synchronization Mode", safe(cfg.writeSynchronizationMode))
        cacheT += ("Swap Enabled", bool2Str(cfg.swapEnabled()))
        cacheT += ("Invalidate", bool2Str(cfg.invalidate()))
        cacheT += ("Start Size", cfg.startSize())

        cacheT += ("Affinity Function", safe(affinityCfg.function()))
        cacheT += ("Affinity Backups", affinityCfg.partitionedBackups())
        cacheT += ("Affinity Partitions", safe(affinityCfg.partitions()))
        cacheT += ("Affinity Exclude Neighbors", safe(affinityCfg.excludeNeighbors()))
        cacheT += ("Affinity Mapper", safe(affinityCfg.mapper()))

        cacheT += ("Rebalance Mode", rebalanceCfg.mode())
        cacheT += ("Rebalance Batch Size", rebalanceCfg.batchSize())
        cacheT += ("Rebalance Thread Pool size", rebalanceCfg.threadPoolSize())
        cacheT += ("Rebalance Timeout", rebalanceCfg.timeout())
        cacheT += ("Rebalance Delay", rebalanceCfg.partitionedDelay())
        cacheT += ("Time Between Rebalance Messages", rebalanceCfg.throttle())

        cacheT += ("Eviction Policy Enabled", bool2Str(evictCfg.policy() != null))
        cacheT += ("Eviction Policy", safe(evictCfg.policy()))
        cacheT += ("Eviction Policy Max Size", safe(evictCfg.policyMaxSize()))
        cacheT += ("Eviction Filter", safe(evictCfg.filter()))
        cacheT += ("Eviction Key Buffer Size", evictCfg.synchronizedKeyBufferSize())
        cacheT += ("Eviction Synchronized", bool2Str(evictCfg.evictSynchronized()))
        cacheT += ("Eviction Overflow Ratio", evictCfg.maxOverflowRatio())
        cacheT += ("Synchronous Eviction Timeout", evictCfg.synchronizedTimeout())
        cacheT += ("Synchronous Eviction Concurrency Level", evictCfg.synchronizedConcurrencyLevel())

        cacheT += ("Near Cache Enabled", bool2Str(nearCfg.nearEnabled()))
        cacheT += ("Near Start Size", nearCfg.nearStartSize())
        cacheT += ("Near Eviction Policy", safe(nearCfg.nearEvictPolicy()))
        cacheT += ("Near Eviction Policy Max Size", safe(nearCfg.nearEvictMaxSize()))

        cacheT += ("Default Lock Timeout", defaultCfg.txLockTimeout())
        cacheT += ("Metadata type count", cfg.typeMeta().size())
        cacheT += ("Cache Interceptor", safe(cfg.interceptor()))

        cacheT += ("Store Enabled", bool2Str(storeCfg.enabled()))
        cacheT += ("Store Class", safe(storeCfg.store()))
        cacheT += ("Store Factory Class", storeCfg.storeFactory())
        cacheT += ("Store Keep Binary", storeCfg match {
            case cfg: VisorCacheStoreConfigurationV2 => cfg.storeKeepBinary()
            case _ => false
        })
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

        cacheT +=("Query Execution Time Threshold", queryCfg.longQueryWarningTimeout())
        cacheT +=("Query Schema Name", queryCfg match {
            case cfg: VisorCacheQueryConfigurationV2 => cfg.sqlSchema()
            case _ => null
        })
        cacheT +=("Query Escaped Names", bool2Str(queryCfg.sqlEscapeAll()))
        cacheT +=("Query Onheap Cache Size", queryCfg.sqlOnheapRowCacheSize())

        val sqlFxs = queryCfg.sqlFunctionClasses()

        val hasSqlFxs = sqlFxs != null && sqlFxs.nonEmpty

        if (!hasSqlFxs)
            cacheT +=("Query SQL functions", NA)

        val indexedTypes = queryCfg.indexedTypes()

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
