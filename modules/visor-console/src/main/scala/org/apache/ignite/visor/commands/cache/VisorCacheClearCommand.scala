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
import org.apache.ignite.internal.visor.cache.{VisorCacheClearTask, VisorCacheClearTaskArg}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import scala.language.reflectiveCalls

/**
 * ==Overview==
 * Visor 'clear' command implementation.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------------+
 * | cache -clear | Clears all entries from cache on all nodes. |
 * +------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache -clear
 *     cache -clear -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, default cache will be cleared.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cache -clear
 *         Clears interactively selected cache.
 *     cache -clear -c=cache
 *         Clears cache with name 'cache'.
 * }}}
 */
class VisorCacheClearCommand {
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
     * Clears cache by its name.
     *
     * ===Examples===
     * <ex>cache -clear -c=cache</ex>
     * Clears cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def clear(argLst: ArgList, node: Option[ClusterNode]) {
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
            val nid = groupForDataNode(node, cacheName).forRandom().node().id()

            val t = VisorTextTable()

            t #= ("Node ID8(@)", "Cache Size Before", "Cache Size After")

            val res = executeOne(nid, classOf[VisorCacheClearTask], new VisorCacheClearTaskArg(cacheName))

            t += (nodeId8(nid), res.getSizeBefore, res.getSizeAfter)

            println("Cleared cache with name: " + escapeName(cacheName))

            t.render()
        }
        catch {
            case e: ClusterGroupEmptyException => scold(messageNodeNotFound(node, cacheName))
            case e: Throwable =>  scold(e)
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheClearCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheClearCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
