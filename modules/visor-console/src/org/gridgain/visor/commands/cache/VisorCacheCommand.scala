/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.cache

import java.util
import java.util.UUID
import org.jetbrains.annotations._
import scala.collection._
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.gridgain.grid._
import org.gridgain.grid.compute.{GridComputeJobResult, GridComputeJobAdapter, GridComputeJob}
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.util.scala.impl
import org.gridgain.grid.util.typedef._
import org.gridgain.grid.cache._
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.resources.GridInstanceResource
import org.gridgain.scalar.scalar._
import org.gridgain.visor._
import visor._
import org.gridgain.visor.commands.{VisorConsoleMultiNodeTask, VisorConsoleCommand, VisorTextTable}

/**
 * ==Overview==
 * Visor 'cache' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.cache.VisorCacheCommand._
 * </ex>
 * Note that `VisorCacheCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +--------------------------------------------------------------------------------+
 * | cache | Prints statistics about caches from specified node on the entire grid. |
 * |       | Output sorting can be specified in arguments.                          |
 * |       |                                                                        |
 * |       | Output abbreviations:                                                  |
 * |       |     #   Number of nodes.                                               |
 * |       |     H/h Number of cache hits.                                          |
 * |       |     M/m Number of cache misses.                                        |
 * |       |     R/r Number of cache reads.                                         |
 * |       |     W/w Number of cache writes.                                        |
 * +--------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor cache
 *     visor cache "-i {-n=<name>}"
 *     visor cache "{-n=<name>} {-id=<node-id>|id8=<node-id8>} {-s=lr|lw|hi|mi|re|wr} {-a} {-r}"
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
 *     -n=<name>
 *         Name of the cache.
 *         By default - statistics for all caches will be printed.
 *     -s=no|lr|lw|hi|mi|re|wr
 *         Defines sorting type. Sorted by:
 *            lr Last read.
 *            lw Last write.
 *            hi Hits.
 *            mi Misses.
 *            rd Reads.
 *            wr Writes.
 *         If not specified - default sorting is 'lr'.
 *     -i
 *         Interactive mode.
 *         User can interactively select node for cache statistics.
 *     -r
 *         Defines if sorting should be reversed.
 *         Can be specified only with '-s' argument.
 *     -a
 *         Prints details statistics about each cache.
 *         By default only aggregated summary is printed.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor cache "-id8=12345678 -s=hi -r"
 *         Prints summary statistics about caches from node with specified id8
 *         sorted by number of hits in reverse order.
 *     visor cache "-i"
 *         Prints cache statistics for interactively selected node.
 *     visor cache "-s=hi -r -a"
 *         Prints detailed statistics about all caches sorted by number of hits in reverse order.
 *     visor cache
 *         Prints summary statistics about all caches.
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
     * <ex>cache "-id8=12345678 -s=no -r"</ex>
     * Prints statistics about caches from node with specified id8 sorted by number of
     * nodes in reverse order.
     *
     * <ex>cache "-s=no -r"</ex>
     * Prints statistics about all caches sorted by number of nodes in reverse order.
     *
     * @param args Command arguments.
     */
    def cache(args: String) {
        breakable {
            if (!isConnected)
                adviseToConnect()
            else {
                val argLst = parseArgs(args)

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

                val id8 = argValue("id8", argLst)
                val id = argValue("id", argLst)
                val name = argValue("n", argLst)
                val all = hasArgFlag("a", argLst)

                var node: Option[GridNode] = None

                if (id8.isDefined && id.isDefined)
                    scold("Only one of '-id8' or '-id' is allowed.").^^

                if (id8.isDefined) {
                    val ns = nodeById8(id8.get)

                    if (ns.isEmpty)
                        scold("Unknown 'id8' value: " + id8.get).^^
                    else if (ns.size != 1)
                        scold("'id8' resolves to more than one node (use full 'id' instead): " + id8.get).^^
                    else
                        node = ns.headOption
                }
                else if (id.isDefined)
                    try {
                        node = Option(grid.node(java.util.UUID.fromString(id.get)))

                        if (!node.isDefined)
                            scold("'id' does not match any node: " + id.get).^^
                    }
                    catch {
                        case e: IllegalArgumentException => scold("Invalid node 'id': " + id.get).^^
                    }

                val sortType = argValue("s", argLst)
                val reversed = hasArgName("r", argLst)

                if (sortType.isDefined && !isValidSortType(sortType.get))
                    scold("Invalid '-s' argument in: " + args).^^

                // Get cache stats data from all nodes.
                val aggrData = cacheData(node, name)

                if (aggrData.isEmpty)
                    scold("No caches found.").^^

                println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

                val sumT = VisorTextTable()

                sumT #= (("Name(@),", "Last Read/Write"), "Nodes", "Size", "Hits", "Misses", "Reads", "Writes")

                sortAggregatedData(aggrData, sortType getOrElse "lr", reversed).foreach(
                    ad => {
                        // Add cache host as visor variable.
                        registerCacheName(ad.cacheName)

                        sumT += (
                            (
                                mkCacheName(ad.cacheName),
                                " ",
                                formatDateTime(ad.lastRead),
                                formatDateTime(ad.lastWrite)
                                ),
                            ad.nodes,
                            (
                                "min: " + ad.minSize,
                                "avg: " + formatDouble(ad.avgSize),
                                "max: " + ad.maxSize
                                ),
                            (
                                "min: " + ad.minHits,
                                "avg: " + formatDouble(ad.avgHits),
                                "max: " + ad.maxHits
                                ),
                            (
                                "min: " + ad.minMisses,
                                "avg: " + formatDouble(ad.avgMisses),
                                "max: " + ad.maxMisses
                                ),
                            (
                                "min: " + ad.minReads,
                                "avg: " + formatDouble(ad.avgReads),
                                "max: " + ad.maxReads
                                ),
                            (
                                "min: " + ad.minWrites,
                                "avg: " + formatDouble(ad.avgWrites),
                                "max: " + ad.maxWrites
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

                    sorted.foreach(ad => {
                        val cacheNameVar = mkCacheName(ad.cacheName)

                        println("\nCache '" + cacheNameVar + "':")

                        val csT = VisorTextTable()

                        csT += ("Name(@)", cacheNameVar)
                        csT += ("Nodes", ad.nodes.size)
                        csT += ("Size Min/Avg/Max", ad.minSize + " / " + formatDouble(ad.avgSize) + " / " + ad.maxSize)

                        val ciT = VisorTextTable()

                        ciT #= ("Node ID8(@), IP", "CPUs", "Heap Used", "CPU Load", "Up Time", "Size",
                            "Last Read/Write", "Hi/Mi/Rd/Wr")

                        sortData(ad.data, sortType getOrElse "lr", reversed).
                            foreach(cd => {
                                ciT += (
                                    nodeId8Addr(cd.nodeId),
                                    cd.cpus,
                                    formatDouble(cd.heapUsed) + " %",
                                    formatDouble(cd.cpuLoad) + " %",
                                    X.timeSpan2HMSM(cd.upTime),
                                    cd.size,
                                    (
                                        formatDateTime(cd.lastRead),
                                        formatDateTime(cd.lastWrite)
                                        ),
                                    (
                                        "Hi: " + cd.hits,
                                        "Mi: " + cd.misses,
                                        "Rd: " + cd.reads,
                                        "Wr: " + cd.writes
                                        )
                                    )
                            })

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
                        val qm = ad.qryMetrics

                        nl()
                        println("Aggregated queries metrics:")
                        println("  Minimum execution time: " + X.timeSpan2HMSM(qm.minTime))
                        println("  Maximum execution time: " + X.timeSpan2HMSM(qm.maxTime))
                        println("  Average execution time: " + X.timeSpan2HMSM(qm.avgTime.toLong))
                        println("  Total number of executions: " + qm.execs)
                        println("  Total number of failures:   " + qm.fails)
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
        if (s == null)
            "<default>"
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
    private def registerCacheName(@Nullable s: String) =
        if (s != null)
            setVarIfAbsent(s, "c")

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
    private def cacheData(node: Option[GridNode], name: Option[String]): List[VisorCacheAggregatedData] = {
        assert(node != null)

        try {
            val prj = if (node.isDefined) grid.forNode(node.get) else grid

            prj.compute().execute(classOf[VisorCacheDataTask], name).get().toList
        }
        catch {
            case e: GridException => Nil
        }
    }

    /**
     * Tests whether passed in parameter is a valid sorting type.
     *
     * @param arg Sorting type to test.
     */
    private def isValidSortType(arg: String): Boolean = {
        assert(arg != null)

        Set("lr", "lw", "hi", "mi", "rd", "wr").contains(arg.trim)
    }

    /**
     * Sort metrics data.
     *
     * @param data Unsorted list.
     * @param arg Sorting command argument.
     * @param reverse Whether to reverse sorting or not.
     * @return Sorted data.
     */
    private def sortData(data: Iterable[VisorCacheData], arg: String, reverse: Boolean): List[VisorCacheData] = {
        assert(data != null)
        assert(arg != null)

        val sorted = arg.trim match {
            case "lr" => data.toList.sortBy(_.lastRead)
            case "lw" => data.toList.sortBy(_.lastWrite)
            case "hi" => data.toList.sortBy(_.hits)
            case "mi" => data.toList.sortBy(_.misses)
            case "rd" => data.toList.sortBy(_.reads)
            case "wr" => data.toList.sortBy(_.writes)

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
    private def sortAggregatedData(data: Iterable[VisorCacheAggregatedData], arg: String, reverse: Boolean):
        List[VisorCacheAggregatedData] = {

        val sorted = arg.trim match {
            case "lr" => data.toList.sortBy(_.lastRead)
            case "lw" => data.toList.sortBy(_.lastWrite)
            case "hi" => data.toList.sortBy(_.avgHits)
            case "mi" => data.toList.sortBy(_.avgMisses)
            case "rd" => data.toList.sortBy(_.avgReads)
            case "wr" => data.toList.sortBy(_.avgWrites)

            case _ =>
                assert(false, "Unknown sorting type: " + arg)

                Nil
        }

        if (reverse) sorted.reverse else sorted
    }
}

/**
 * Task that runs on all nodes and returns cache metrics data.
 */
@GridInternal
private class VisorCacheDataTask extends VisorConsoleMultiNodeTask[Option[String], Iterable[VisorCacheAggregatedData]] {
    @impl def job(name: Option[String]): GridComputeJob = new GridComputeJobAdapter() {
        /** Injected grid */
        @GridInstanceResource
        private val g: GridEx = null

        override def execute(): AnyRef = {
            val caches: Iterable[GridCache[_, _]] = name match {
                case Some(n) => Seq(g.cachex(n))
                case None => g.cachesx()
            }

            if (caches != null)
                caches.collect {
                    case c =>
                        val m = g.localNode.metrics
                        val qm = c.queries().metrics()

                        VisorCacheData(
                            cacheName = c.name,
                            nodeId = g.localNode.id,
                            cpus = m.getTotalCpus,
                            heapUsed = m.getHeapMemoryUsed / m.getHeapMemoryMaximum * 100,
                            cpuLoad = m.getCurrentCpuLoad * 100,
                            upTime = m.getUpTime,
                            size = c.size,
                            lastRead = c.metrics.readTime,
                            lastWrite = c.metrics.writeTime,
                            hits = c.metrics.hits,
                            misses = c.metrics.misses,
                            reads = c.metrics.reads,
                            writes = c.metrics.writes,
                            VisorCacheQueryMetrics(qm.minimumTime(), qm.maximumTime(), qm.averageTime(),
                                qm.executions(), qm.fails())
                        )
                }.toSeq
            else
                Seq.empty[VisorCacheData]

        }
    }

    override def reduce(results: util.List[GridComputeJobResult]): Iterable[VisorCacheAggregatedData] = {
        val aggrData = mutable.Map.empty[String, VisorCacheAggregatedData]

        for (res <- results if res.getException == null) {
            for (cd <- res.getData[Seq[VisorCacheData]]) {
                val ad = aggrData.getOrElse(cd.cacheName, VisorCacheAggregatedData(cd.cacheName))

                ad.data = ad.data :+ cd

                ad.nodes = nodeId8Addr(cd.nodeId) +: ad.nodes

                ad.minSize = math.min(ad.minSize, cd.size)
                ad.maxSize = math.max(ad.maxSize, cd.size)
                ad.lastRead = math.max(ad.lastRead, cd.lastRead)
                ad.lastWrite = math.max(ad.lastWrite, cd.lastWrite)
                ad.minHits = math.min(ad.minHits, cd.hits)
                ad.maxHits = math.max(ad.maxHits, cd.hits)
                ad.minMisses = math.min(ad.minMisses, cd.misses)
                ad.maxMisses = math.max(ad.maxMisses, cd.misses)
                ad.minReads = math.min(ad.minReads, cd.reads)
                ad.maxReads = math.max(ad.maxReads, cd.reads)
                ad.minWrites = math.min(ad.minWrites, cd.writes)
                ad.maxWrites = math.max(ad.maxWrites, cd.writes)

                // Partial aggregation of averages.
                ad.avgWrites += cd.writes
                ad.avgReads += cd.reads
                ad.avgMisses += cd.misses
                ad.avgHits += cd.hits
                ad.avgSize += cd.size

                // Aggregate query metrics data
                val qm = cd.qryMetrics
                val aqm = ad.qryMetrics

                aqm.minTime = if (aqm.minTime > 0) math.min(qm.minTime, aqm.minTime) else qm.minTime
                aqm.maxTime = math.max(qm.maxTime, aqm.maxTime)
                aqm.execs += qm.execs
                aqm.fails += qm.fails
                aqm.totalTime += (qm.avgTime * qm.execs).toLong

                aggrData.put(cd.cacheName, ad)
            }
        }

        // Final aggregation of averages.
        aggrData.values foreach (ad => {
            val n = ad.nodes.size

            ad.avgSize /= n
            ad.avgHits /= n
            ad.avgMisses /= n
            ad.avgReads /= n
            ad.avgWrites /= n

            val aqm = ad.qryMetrics

            aqm.avgTime = if (aqm.execs > 0) aqm.totalTime / aqm.execs else 0
        })

        aggrData.values
    }
}

/**
 * Cache metrics data.
 */
private case class VisorCacheData(
    cacheName: String,
    nodeId: UUID,
    cpus: Int,
    heapUsed: Double,
    cpuLoad: Double,
    upTime: Long,
    size: Int,
    lastRead: Long,
    lastWrite: Long,
    hits: Int,
    misses: Int,
    reads: Int,
    writes: Int,
    qryMetrics: VisorCacheQueryMetrics
)

/**
 * Aggregated cache metrics data.
 */
private case class VisorCacheAggregatedData(
    cacheName: String,
    var nodes: Seq[String] = Seq.empty[String],
    var minSize: Int = Int.MaxValue,
    var avgSize: Double = 0.0,
    var maxSize: Int = 0,
    var lastRead: Long = 0,
    var lastWrite: Long = 0,
    var minHits: Int = Int.MaxValue,
    var avgHits: Double = 0.0,
    var maxHits: Int = 0,
    var minMisses: Int = Int.MaxValue,
    var avgMisses: Double = 0.0,
    var maxMisses: Int = 0,
    var minReads: Int = Int.MaxValue,
    var avgReads: Double = 0.0,
    var maxReads: Int = 0,
    var minWrites: Int = Int.MaxValue,
    var avgWrites: Double = 0.0,
    var maxWrites: Int = 0,
    qryMetrics: VisorAggregatedCacheQueryMetrics = VisorAggregatedCacheQueryMetrics(),
    var data: Seq[VisorCacheData] = Seq.empty[VisorCacheData]
)

/**
 * Cache query metrics data.
 */
private case class VisorCacheQueryMetrics(
    minTime: Long,
    maxTime: Long,
    avgTime: Double,
    execs: Int,
    fails: Int
)

/**
 * Aggregated cache query metrics data.
 */
private case class VisorAggregatedCacheQueryMetrics(
    var minTime: Long = 0,
    var maxTime: Long = 0,
    var avgTime: Double = 0,
    var totalTime: Long = 0,
    var execs: Int = 0,
    var fails: Int = 0
)

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheCommand {
    addHelp(
        name = "cache",
        shortInfo = "Prints cache statistics.",
        longInfo = Seq(
            "Prints statistics about caches from specified node on the entire grid.",
            "Output sorting can be specified in arguments.",
            " ",
            "Output abbreviations:",
            "    #   Number of nodes.",
            "    H/h Number of cache hits.",
            "    M/m Number of cache misses.",
            "    R/r Number of cache reads.",
            "    W/w Number of cache writes."
        ),
        spec = Seq(
            "cache",
            "cache -i",
            "cache {-n=<cache-name>} {-id=<node-id>|id8=<node-id8>} {-s=lr|lw|hi|mi|re|wr} {-a} {-r}"
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
            "-n=<cache-name>" -> Seq(
                "Name of the cache.",
                "By default - statistics for all caches will be printed.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            ),
            "-s=no|lr|lw|hi|mi|re|wr" -> Seq(
                "Defines sorting type. Sorted by:",
                "   lr Last read.",
                "   lw Last write.",
                "   hi Hits.",
                "   mi Misses.",
                "   rd Reads.",
                "   wr Writes.",
                "If not specified - default sorting is 'lr'."
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
            )
        ),
        examples = Seq(
            "cache -id8=12345678 -s=hi -r"  -> Seq(
                "Prints summary statistics about caches from node with specified id8",
                "sorted by number of hits in reverse order."
            ),
            "cache -id8=@n0 -s=hi -r"  -> Seq(
                "Prints summary statistics about caches from node with id8 taken from 'n0' memory variable.",
                "sorted by number of hits in reverse order."
            ),
            "cache -i" ->
                "Prints cache statistics for interactively selected node.",
            "cache -n=@c0 -a"  -> Seq(
                "Prints detailed statistics about cache with name taken from 'c0' memory variable."
            ),
            "cache -s=hi -r -a" ->
                "Prints detailed statistics about all caches sorted by number of hits in reverse order.",
            "cache" ->
                "Prints summary statistics about all caches."
        ),
        ref = VisorConsoleCommand(cmd.cache, cmd.cache)
    )

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
    implicit def fromCinfo2Visor(vs: VisorTag) = cmd
}
