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
import org.apache.ignite.internal.visor.cache.{VisorCacheResetMetricsTask, VisorCacheResetMetricsTaskArg}
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.visor.visor._

import scala.language.reflectiveCalls

/**
 * ==Overview==
 * Visor 'reset' command implementation.
 *
 * ====Specification====
 * {{{
 *     cache -reset -c=<cache name>
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
 *    cache -reset -c=@c0
 *        Reset metrics for cache with name taken from 'c0' memory variable.
 * }}}
 */
class VisorCacheResetCommand {
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
     * Reset metrics for cache with specified name.
     *
     * ===Examples===
     * <ex>cache -c=cache -reset</ex>
     * Reset metrics for cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def reset(argLst: ArgList, node: Option[ClusterNode]) {
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
            executeRandom(grp, classOf[VisorCacheResetMetricsTask], new VisorCacheResetMetricsTaskArg(cacheName))

            println("Visor successfully reset metrics for cache: " + escapeName(cacheName))
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
object VisorCacheResetCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheResetCommand

    /**
      * Singleton.
      */
    def apply() = cmd
}
