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

import org.apache.ignite.cluster.{ClusterGroupEmptyException, ClusterNode}
import org.apache.ignite.visor.commands.common.VisorTextTable
import org.apache.ignite.visor.visor._

import org.apache.ignite.internal.visor.query._
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import scala.collection.JavaConversions._

/**
 * ==Overview==
 * Visor 'scan' command implementation.
 *
 * ====Specification====
 * {{{
 *     cache -scan -c=<cache name> {-near} {-id=<node-id>|-id8=<node-id8>} {-p=<page size>}
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *     <near>
 *         Prints list of all entries from near cache of cache.
 *     <node-id>
 *         Full node ID.
 *     <node-id8>
 *         Node ID8.
 *     <page size>
 *         Number of object to fetch from cache at once.
 * }}}
 *
 * ====Examples====
 * {{{
 *    cache -c=cache
 *        List entries from cache with name 'cache' from all nodes with this cache.
 *    cache -c=@c0 -scan -p=50
 *        List entries from cache with name taken from 'c0' memory variable with page of 50 items
 *        from all nodes with this cache.
 *    cache -scan -c=cache -id8=12345678
 *        List entries from cache with name 'cache' and node '12345678' ID8.
 *    cache -scan -near -c=cache -id8=12345678
 *        List entries from near cache of cache with name 'cache' and node '12345678' ID8.
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

    private def error(e: Throwable) {
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
    def scan(argLst: ArgList, node: Option[ClusterNode]) {
        val pageArg = argValue("p", argLst)
        val cacheArg = argValue("c", argLst)
        val near = hasArgName("near", argLst)

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

        val firstPage =
            try
                executeRandom(groupForDataNode(node, cacheName),
                    classOf[VisorScanQueryTask], new VisorScanQueryTaskArg(cacheName, null, false, false, near, false, pageSize)) match {
                    case x if x.getError != null =>
                        error(x.getError)

                        return
                    case x if x.getResult.getRows != null =>
                        x.getResult

                    case x =>
                        var res = x.getResult

                        Thread.sleep(100)

                        while (res.getRows == null) {
                            res = executeOne(res.getResponseNodeId, classOf[VisorQueryFetchFirstPageTask],
                                new VisorQueryNextPageTaskArg(res.getQueryId, pageSize)) match {
                                case x if x.getError != null =>
                                    error(x.getError)

                                    return
                                case x => x.getResult
                            }

                            if (res.getRows == null)
                                Thread.sleep(500)
                        }

                        res
                }
            catch {
                case e: ClusterGroupEmptyException =>
                    scold(messageNodeNotFound(node, cacheName))

                    return
                case e: Throwable =>
                    error(e)

                    return
            }

        if (firstPage.getRows.isEmpty) {
            println(s"${if (near) "Near cache" else "Cache"}: ${escapeName(cacheName)} is empty")

            return
        }

        var nextPage: VisorQueryResult = firstPage

        def render() {
            println(s"Entries in ${if (near) "near" else ""} cache: " + escapeName(cacheName))

            val t = VisorTextTable()

            t #= ("Key Class", "Key", "Value Class", "Value")

            nextPage.getRows.foreach(r => t += (r(0), r(1), r(2), r(3)))

            t.render()
        }

        render()

        while (nextPage.isHasMore) {
            ask("\nFetch more objects (y/n) [y]:", "y") match {
                case "y" | "Y" =>
                    try {
                        nextPage = executeOne(firstPage.getResponseNodeId, classOf[VisorQueryNextPageTask],
                            new VisorQueryNextPageTaskArg(firstPage.getQueryId, pageSize))

                        render()
                    }
                    catch {
                        case e: Exception => error(e)
                    }
                case _ => return
            }
        }
    }
}

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
