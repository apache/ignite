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

import java.util.{HashSet => JavaSet}

import org.apache.ignite.cluster.{ClusterGroupEmptyException, ClusterNode}
import org.apache.ignite.internal.visor.cache.{VisorCacheToggleStatisticsTask, VisorCacheToggleStatisticsTaskArg}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.visor.visor._

import scala.language.reflectiveCalls

/**
 * ==Overview==
 * Visor 'statistics' command implementation.
 *
 * ====Specification====
 * {{{
 *     cache -statistics=<on|off> -c=<cache name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <state>
  *        Statistics collection state to set for cache.
 *     <cache-name>
 *         Name of the cache.
 * }}}
 *
 * ====Examples====
 * {{{
 *    cache -statistics=on -c=@c0
 *        Enable collection of statistics for cache with name taken from 'c0' memory variable.
 *    cache -statistics=off -c=@c0
 *        Disable collection of statistics for cache with name taken from 'c0' memory variable.
 * }}}
 */
class VisorCacheToggleStatisticsCommand {
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
     * Toggle statistics collection for cache with specified name.
     *
     * ===Examples===
     * <ex>cache -statistics=on -c=cache</ex>
     * Enable collection of statistics for cache with name 'cache'.
     * <ex>cache -statistics=off -c=cache</ex>
     * Disable collection of statistics for cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def toggle(argLst: ArgList, node: Option[ClusterNode]) {
        val cacheArg = argValue("c", argLst)

        val cacheName = cacheArg match {
            case None => null // default cache.

            case Some(s) if s.startsWith("@") =>
                warn("Can't find cache variable with specified name: " + s,
                    "Type 'cache' to see available cache variables."
                )

                return

            case Some(name) => name
        }

        val grp = try {
            groupForDataNode(node, cacheName)
        }
        catch {
            case _: ClusterGroupEmptyException =>
                scold(messageNodeNotFound(node, cacheName))

                return
        }

        try {
            val cacheNames = new JavaSet[String]()
            cacheNames.add(cacheName)

            val enable = argValue("statistics", argLst) match {
                case Some(state) if "on".equalsIgnoreCase(state) => true
                case Some(state) if "off".equalsIgnoreCase(state) => false
                case _ =>
                    warn("Goal state for collection of cache statistics is not specified.",
                        "Use \"on\" and \"off\" value of \"statistics\" argument to toggle collection of cache statistics.")

                    return
            }

            executeRandom(grp, classOf[VisorCacheToggleStatisticsTask],
                new VisorCacheToggleStatisticsTaskArg(enable, cacheNames))

            println("Visor successfully " + (if (enable) "enable" else "disable") +
                " collection of statistics for cache: " + escapeName(cacheName))
        }
        catch {
            case _: ClusterGroupEmptyException => scold(messageNodeNotFound(node, cacheName))
            case e: Exception => error(e)
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheToggleStatisticsCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheToggleStatisticsCommand

    /**
      * Singleton.
      */
    def apply() = cmd
}
