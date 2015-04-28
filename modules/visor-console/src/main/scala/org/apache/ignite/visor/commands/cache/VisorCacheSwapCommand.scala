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

import java.util.Collections

import org.apache.ignite.internal.visor.cache.VisorCacheSwapBackupsTask
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}

/**
 * ==Overview==
 * Visor 'cswap' command implementation.
 *
 * ==Help==
 * {{{
 * +-----------------------------------------------------+
 * | cswap | Swaps backup entries in cache on all nodes. |
 * +-----------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache -swap
 *     cache -swap -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, entries in default cache will be swapped.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cache -swap
 *         Swaps entries in interactively selected cache.
 *     cache -swap -c=cache
 *         Swaps entries in cache with name 'cache'.
 * }}}
 */
class VisorCacheSwapCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cswap' to see how to use this command.")
    }

    /**
     * ===Command===
     * Swaps entries in cache.
     *
     * ===Examples===
     * <ex>cache -swap -c=cache</ex>
     * Swaps entries in cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def swap(argLst: ArgList, node: Option[ClusterNode]) {
        val cacheArg = argValue("c", argLst)

        val cacheName = cacheArg match {
            case None => null // default cache.

            case Some(s) if s.startsWith("@") =>
                warn("Can't find cache variable with specified name: " + s,
                    "Type 'cache' to see available cache variables.")

                return

            case Some(name) => name
        }

        try {
            val grp = groupForDataNode(node, cacheName)

            val t = VisorTextTable()

            t #=("Node ID8(@)", "Entries Swapped", "Cache Size Before", "Cache Size After")

            for (node <- grp.nodes()) {
                val nid = node.id()

                val r = executeOne(nid, classOf[VisorCacheSwapBackupsTask], Collections.singleton(cacheName)).
                    get(cacheName)

                if (r != null)
                    t +=(nodeId8(nid), r.get1() - r.get2(), r.get1(), r.get2())
            }

            if (t.nonEmpty) {
                println("Swapped entries in cache: " + escapeName(cacheName))

                t.render()
            }
            else
                scold(messageNodeNotFound(node, cacheName))
        }
        catch {
            case e: ClusterGroupEmptyException => scold(messageNodeNotFound(node, cacheName))
            case e: Throwable => scold(e)
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheSwapCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheSwapCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
