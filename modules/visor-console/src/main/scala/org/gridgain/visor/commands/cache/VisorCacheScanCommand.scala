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

import java.util.{Map => JavaMap, UUID}
import collection.JavaConversions._
import org.gridgain.grid.cache.query.GridCacheQueryFuture
import org.gridgain.grid.kernal.{GridInternalException, GridEx}
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.util.{GridUtils => U}
import org.gridgain.grid.util.scala.impl
import org.gridgain.grid.util.typedef.CAX
import org.gridgain.visor.commands._
import org.gridgain.visor.visor._
import org.gridgain.grid.cache.GridCacheMode
import org.gridgain.grid.{GridNode, GridException}

/**
 * ==Overview==
 * Visor 'scan' command implementation.
 *
 * ====Specification====
 * {{{
 *     cache {-id=<node-id>|-id8=<node-id8>} {-p=<page size>} -c=<cache name> -scan
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <node-id>
 *         Full node ID.
 *     <node-id8>
 *         Node ID8.
 *     <page size>
 *         Number of object to fetch from cache at once.
 *     <cache-name>
 *         Name of the cache.
 * }}}
 *
 * ====Examples====
 * {{{
 *    cache -c=cache
 *        List entries from cache with name 'cache' from all nodes with this cache.
 *    cache -c=@c0 -scan -p=50
 *        List entries from cache with name taken from 'c0' memory variable with page of 50 items
 *        from all nodes with this cache.
 *    cache -c=cache -scan -id8=12345678
 *        List entries from cache with name 'cache' and node '12345678' ID8.
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
        warn("Type 'help cache' to see how to use this command.")
    }

    private def error(e: Exception) {
        var cause: Throwable = e

        while (cause.getCause != null)
            cause = cause.getCause

        scold(cause.getMessage)
    }

    /**
     * ===Command===
     * List all entries in cache with specified name.
     *
     * ===Examples===
     * <ex>cache -c=cache -scan</ex>
     *     List entries from cache with name 'cache' from all nodes with this cache.
     * <br>
     * <ex>cache -c=@c0 -scan -p=50</ex>
     *     List entries from cache with name taken from 'c0' memory variable with page of 50 items
     *     from all nodes with this cache.
     * <br>
     * <ex>cache -c=cache -scan -id8=12345678</ex>
     *     List entries from cache with name 'cache' and node '12345678' ID8.
     *
     * @param argLst Command arguments.
     */
    def scan(argLst: ArgList, node: Option[GridNode]) {
        val pageArg = argValue("p", argLst)
        val cacheArg = argValue("c", argLst)

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

        val cacheName = cacheArg match {
            case None => null // default cache.

            case Some(s) if s.startsWith("@") =>
                warn("Can't find cache variable with specified name: " + s,
                    "Type 'cache' to see available cache variables."
                )

                return

            case Some(name) => name
        }

        val cachePrj = node match {
            case Some(n) => grid.forNode(n).forCache(cacheName)
            case _ => grid.forCache(cacheName)
        }

        if (cachePrj.nodes().isEmpty) {
            warn("Can't find nodes with specified cache: " + cacheName,
                "Type 'cache' to see available cache names."
            )

            return
        }

        val qryPrj = cachePrj.forRandom()

        val nid = qryPrj.node().id()

        var res =
            try
                qryPrj
                .compute()
                .withName("visor-cscan-task")
                .withNoFailover()
                .execute(classOf[VisorScanCacheTask],
                    new VisorScanCacheTaskArgs(nid, cachePrj.nodes().map(_.id()).toSeq, cacheName, pageSize))
                .get match {
                    case Left(e) =>
                        error(e)

                        return
                    case Right(r) => r
                }
            catch {
                case e: Exception =>
                    error(e)

                    return
            }

        def escapeCacheName(name: String) = if (name == null) "<default>" else name

        if (res.rows.isEmpty) {
            println("Cache: " + escapeCacheName(cacheName) + " is empty")

            return
        }

        def render() {
            println("Entries in cache: " + escapeCacheName(cacheName))

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

object VisorScanCache {
    type VisorScanStorageValType = (GridCacheQueryFuture[JavaMap.Entry[Object, Object]], JavaMap.Entry[Object, Object],
        Int, Boolean)

    type VisorScanResult = (String, String, String, String)

    final val SCAN_QRY_KEY = "VISOR_CONSOLE_SCAN_CACHE"

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
        def typeOf(o: Object): String = {
            if (o != null) {
                val clazz = o.getClass

                if (clazz.isArray)
                    U.compact(clazz.getComponentType.getName) + "[]"
                else
                    U.compact(o.getClass.getName)
            }
            else
                "n/a"
        }

        def valueOf(o: Object): String = {
            def mkstring(arr: Array[_], maxSz: Int) = {
                val end = "..."
                val sep = ", "

                val sb = new StringBuilder

                var first = true

                arr.takeWhile(v => {
                    if (first)
                        first = false
                    else
                        sb.append(sep)

                    sb.append(v)

                    sb.size < maxSz
                })

                if (sb.size >= maxSz) {
                    sb.setLength(maxSz - end.size)

                    sb.append(end)
                }

                sb.toString()
            }

            o match {
                case null => "null"
                case b: Array[Byte] => "size=" + b.length
                case arr: Array[_] => "size=" + arr.length + ", values=[" + mkstring(arr, 30) + "]"
                case v => v.toString
            }
        }

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
private class VisorScanCacheTask extends VisorConsoleOneNodeTask[VisorScanCacheTaskArgs, Either[Exception, VisorScanCacheResult]] {
    @impl protected def run(g: GridEx, arg: VisorScanCacheTaskArgs): Either[Exception, VisorScanCacheResult] = {
        val nodeLclKey = SCAN_QRY_KEY + "-" + UUID.randomUUID()

        try {
            val c = g.cachex[Object, Object](arg.cacheName)

            if (c.configuration().getCacheMode == GridCacheMode.LOCAL && arg.proj.size > 1)
                Left(new GridException("Distributed queries are not available for caches with LOCAL caching mode. " +
                    "Please specify only one node to query."))
            else {
                val fut = c.queries().createScanQuery(null)
                    .pageSize(arg.pageSize)
                    .projection(g.forNodeIds(arg.proj))
                    .execute()

                val (rows, next) = fetchRows(fut, null, arg.pageSize)

                g.nodeLocalMap[String, VisorScanStorageValType]().put(nodeLclKey, (fut, next, arg.pageSize, false))

                scheduleRemoval(g, nodeLclKey)

                Right(new VisorScanCacheResult(g.localNode().id(), nodeLclKey, rows, next != null))
            }
        }
        catch {
            case e: Exception => Left(e)
        }
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
    /** Singleton command. */
    private val cmd = new VisorCacheScanCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
