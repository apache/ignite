// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.cscan

import java.util.{Map => JavaMap, UUID}
import collection.JavaConversions._
import org.gridgain.grid.cache.query.GridCacheQueryFuture
import org.gridgain.grid.kernal.{GridInternalException, GridEx}
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.util.{GridUtils => U}
import org.gridgain.grid.util.scala.impl
import org.gridgain.grid.util.typedef.CAX
import org.gridgain.visor._
import org.gridgain.visor.commands._
import org.gridgain.visor.visor._

/**
 * ==Overview==
 * Visor 'cscan' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.cscan.VisorCacheScanCommand._
 * </ex>
 * Note that `VisorCacheScanCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------+
 * | cscan | List all entries from cache on all nodes.    |
 * +------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor cscan
 *     visor cscan "<cache-name>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor cscan "cache"
 *         List all entries in cache with name 'cache'.
 * }}}
 */
class VisorCacheScanCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cscan' to see how to use this command.")
    }

    /**
     * ===Command===
     * List all entries in cache with specified name.
     *
     * ===Examples===
     * <ex>cscan "cache"</ex>
     * List all entries in cache with name 'cache'.
     *
     * @param cacheName Cache name.
     */
    def cscan(cacheName: String) {
        if (!isConnected)
            adviseToConnect()
        else {
            if (cacheName == null || cacheName.isEmpty) {
                scold("Cache name is empty.")
                
                return
            }

            val cache = getVariable(cacheName)

            val cachePrj = grid.forCache(cache)
            val qryPrj = cachePrj.forRandom()
            val nid = qryPrj.node().id()

            if (cachePrj.nodes().isEmpty) {
                scold("Can't find nodes with specified cache: " + cache)

                return
            }

            var res = qryPrj
                .compute()
                .withName("visor-cscan-task")
                .withNoFailover()
                .execute(classOf[VisorScanCacheTask],
                    new VisorScanCacheTaskArgs(nid, cachePrj.nodes().map(_.id()).toSeq, cache, 25))
                .get
            
            if (res.rows.isEmpty) {
                println("Cache is empty")
                
                return
            }

            def render() {
                val t = VisorTextTable()

                t #= ("Key Class", "Key", "Value Class", "Value")

                    res.rows.foreach(r => t += (r._1, r._2, r._3, r._4))

                    t.render()
            }
            
            render()
            
            while (res.hasMore) {
                ask("\nFetch more objects (y/n) [y]:", "y") match {
                    case "y" | "Y" =>
                        res = qryPrj
                            .compute()
                            .withName("visor-cscan-fetch-task")
                            .withNoFailover()
                            .execute(classOf[VisorFetchNextPageTask], new VisorFetchNextPageTaskArgs(nid, res.nlKey, 25))
                            .get

                        render()

                    case _ => return
                }

            }
        }
    }

    def cscan() {
        scold("Cache name required")
    }
}

object VisorScanCache {
    type VisorScanStorageValType = (GridCacheQueryFuture[JavaMap.Entry[Object, Object]], JavaMap.Entry[Object, Object],
        Int, Boolean)
    
    type VisorScanResult = (String, String, String, String)

    final val SCAN_QUERY_KEY = "VISOR_CONSOLE_SCAN_CACHE"

    /** How long to store future. */
    final val RMV_DELAY: Int = 5 * 60 // 5 minutes.

    /**
     * Fetch rows from SCAN query future.
     *
     * @param fut Query future to fetch rows from.
     * @param savedNext Last processed element from future.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows and last processed element.
     */
    def fetchRows(fut: GridCacheQueryFuture[JavaMap.Entry[Object, Object]],
        savedNext: JavaMap.Entry[Object, Object],
        pageSize: Int): (Array[VisorScanResult], JavaMap.Entry[Object, Object]) = {
        def typeOf(o: Object): String = if (o != null) U.compact(o.getClass.getName) else "null"

        def valueOf(o: Object): String =
            if (o != null) {
                if (o.getClass.isArray) "binary" else  o.toString
            }
            else
                "null"

        val rows = collection.mutable.ArrayBuffer.empty[VisorScanResult]

        var cnt = 0

        var next = if (savedNext != null) savedNext else fut.next

        while (next != null && cnt < pageSize) {
            val k = next.getKey
            val v = next.getValue

            val row = (typeOf(k), valueOf(k), typeOf(v), valueOf(v))

            rows += row

            cnt += 1

            next = fut.next
        }

        (rows.toArray, next)
    }
}

import VisorScanCache._

/**
 * Executes SCAN or SQL query and get first page of results.
 */
@GridInternal
private class VisorScanCacheTask extends VisorConsoleOneNodeTask[VisorScanCacheTaskArgs, VisorScanCacheResult] {
    @impl protected def run(g: GridEx, arg: VisorScanCacheTaskArgs): VisorScanCacheResult = {
        val nodeLclKey = SCAN_QUERY_KEY + "-" + UUID.randomUUID()

        val c = g.cache[Object, Object](arg.cacheName)

        val fut = c.queries().createScanQuery(null)
            .pageSize(arg.pageSize)
            .projection(g.forNodeIds(arg.proj))
            .execute()


        val (rows, next) = fetchRows(fut, null, arg.pageSize)

        g.nodeLocalMap[String, VisorScanStorageValType]().put(nodeLclKey, (fut, next, arg.pageSize, false))

        scheduleRemoval(g, nodeLclKey)

        new VisorScanCacheResult(g.localNode().id(), nodeLclKey, rows, next != null)
    }

    private def scheduleRemoval(g: GridEx, id: String) {
        g.scheduler().scheduleLocal(new CAX() {
            override def applyx() {
                val storage = g.nodeLocalMap[String, VisorScanStorageValType]()

                val t = storage.get(id)

                if (t != null) {
                    // If future was accessed since last scheduling,
                    // set access flag to false and reschedule.
                    val (fut, next, pageSize, accessed) = t

                    if (accessed) {
                        storage.put(id, (fut, next, pageSize, false))

                        scheduleRemoval(g, id)
                    }
                    else
                        storage.remove(id) // Remove stored future otherwise.
                }
            }
        }, "{" + RMV_DELAY + ", 1} * * * * *")
    }
}

/** Arguments for `VisorScanCacheTask` */
private case class VisorScanCacheTaskArgs(@impl nodeId: UUID, proj: Seq[UUID], cacheName: String, pageSize: Int)
    extends VisorConsoleOneNodeTaskArgs

/**
 * Fetch next page task.
 */
private class VisorFetchNextPageTask extends VisorConsoleOneNodeTask[VisorFetchNextPageTaskArgs, VisorScanCacheResult] {
    @impl protected def run(g: GridEx, arg: VisorFetchNextPageTaskArgs): VisorScanCacheResult = {
        val storage = g.nodeLocalMap[String, VisorScanStorageValType]()

        val t = storage.get(arg.nlKey)

        if (t == null)
            throw new GridInternalException("Scan query results are expired.")

        val (fut, savedNext, pageSize, _) = t

        val (rows, next) = fetchRows(fut, savedNext, pageSize)

        val hasMore = next != null

        if (hasMore)
            storage.put(arg.nlKey, (fut, next, pageSize, true))
        else
            storage.remove(arg.nlKey)

        VisorScanCacheResult(arg.nodeId, arg.nlKey, rows, hasMore)
    }
}

/** Arguments for `VisorFetchNextPageTask` */
private case class VisorFetchNextPageTaskArgs(@impl nodeId: UUID, nlKey: String, pageSize: Int)
    extends VisorConsoleOneNodeTaskArgs

/**
 * Result of `VisorFieldsQueryTask`.
 *
 * @param nid Node where query executed.
 * @param nlKey Key for later query future extraction.
 * @param rows First page of rows fetched from query.
 * @param hasMore `True` if there is more data could be fetched.
 */
case class VisorScanCacheResult(nid: UUID, nlKey: String, rows: Array[VisorScanResult], hasMore: Boolean)

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheScanCommand {
    addHelp(
        name = "cscan",
        shortInfo = "List all entries from cache on all nodes.",
        spec = Seq(
            "cscan <cache-name>"
        ),
        args = Seq(
            "<cache-name>" -> Seq(
                "Name of the cache.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = Seq(
            "cscan cache" -> "List entries from cache with name 'cache'.",
            "cscan @c0" -> "List entries from cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.cscan, cmd.cscan)
    )

    /** Singleton command. */
    private val cmd = new VisorCacheScanCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromcscan2Visor(vs: VisorTag) = cmd
}
