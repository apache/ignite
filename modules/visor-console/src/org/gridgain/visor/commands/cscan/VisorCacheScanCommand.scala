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

    private def error(e: Exception) {
        var cause: Throwable = e

        while (cause.getCause != null)
            cause = cause.getCause

        val s = cause.getMessage

        println(s)

        scold(cause.getMessage)
    }

    /**
     * ===Command===
     * List all entries in cache with specified name.
     *
     * ===Examples===
     * <ex>cscan -c=cache</ex>
     * List all entries in cache with name 'cache'.
     *
     * @param args Command arguments.
     */
    def cscan(args: String) {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val id8Arg = argValue("id8", argLst)
            val idArg = argValue("id", argLst)
            val pageArg = argValue("p", argLst)
            val cacheArg = argValue("c", argLst)

            var nid: UUID = null

            if (id8Arg.isDefined) {
                val id8 = id8Arg.get
                val ns = nodeById8(id8)

                if (ns.isEmpty)
                    scold("Unknown 'id8' value: " + id8)
                else if (ns.size != 1)
                    scold("'id8' resolves to more than one node (use full 'id' instead): " + id8)
                else
                    nid = ns.head.id

                if (nid == null)
                    return
            }
            else if (idArg.isDefined) {
                val id = idArg.get
                try {
                    val node = grid.node(UUID.fromString(id))

                    if (node != null)
                        nid = node.id
                    else
                        scold("'id' does not match any node: " + id)
                }
                catch {
                    case e: IllegalArgumentException => scold("Invalid node 'id': " + id)
                }

                if (nid == null)
                    return
            }

            var pageSize = 25

            if (pageArg.isDefined) {
                val page = pageArg.get

                try
                 pageSize = page.toInt
                catch {
                    case nfe: NumberFormatException =>
                        scold("Invalid value for 'page size': " + page)

                        return
                }

                if (pageSize < 1 || pageSize > 100) {
                    scold("'Page size' should be in range [1..100] but found: " + page)

                    return
                }
            }

            var cacheName = ""

            if (cacheArg.isDefined)
                cacheName = getVariable(cacheArg.get)
            else {
                scold("Cache name is empty.")

                return
            }

            val cachePrj = if (nid != null) grid.forNodeId(nid).forCache(cacheName) else grid.forCache(cacheName)

            if (cachePrj.nodes().isEmpty) {
                scold("Can't find nodes with specified cache: " + cacheName)

                return
            }

            val qryPrj = cachePrj.forRandom()

            if (nid == null)
                nid = qryPrj.node().id()

            var res =
                try
                    qryPrj
                    .compute()
                    .withName("visor-cscan-task")
                    .withNoFailover()
                    .execute(classOf[VisorScanCacheTask],
                        new VisorScanCacheTaskArgs(nid, cachePrj.nodes().map(_.id()).toSeq, cacheName, pageSize))
                    .get
                catch {
                    case e: Exception =>
                        error(e)

                        return
                }
            
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
                        try
                            res = qryPrj
                                .compute()
                                .withName("visor-cscan-fetch-task")
                                .withNoFailover()
                                .execute(classOf[VisorFetchNextPageTask], new VisorFetchNextPageTaskArgs(nid, res.nlKey, pageSize))
                                .get
                            catch {
                                case e: Exception => error(e)
                            }

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
        shortInfo = "List all entries from cache on specified or all nodes.",
        spec = Seq(
            "cscan",
            "cscan {-id=<node-id>|-id8=<node-id8>} {-p=<page size>} -c=<cache name>"
        ),
        args = Seq(
            "-id=<node-id>" -> Seq(
                "Full node ID.",
                "Either '-id' or '-id8' can be specified.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-id8=<node-id8>" -> Seq(
                "Node ID8.",
                "Note that either '-id8' or '-id' can be specified and " +
                    "you can also use '@n0' ... '@nn' variables as shortcut to <node-id8>.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-p=<page size>" -> Seq(
                "Number of object to fetch from cache at once.",
                "Valid range from 1 to 100.",
                "By default page size is 25."
            ),
            "-c=<cache-name>" -> Seq(
                "Name of the cache.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = Seq(
            "cscan -c=cache" -> "List entries from cache with name 'cache'.",
            "cscan -p=50 -c=@c0" -> "List entries from cache with name taken from 'c0' memory variable with page of 50 items.",
            "cscan -id8=12345678 -c=cache" -> "List entries from cache with name 'cache' and node '12345678' ID8."
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
