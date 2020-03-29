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

import java.util.Collections

import org.apache.ignite.cluster.{ClusterGroupEmptyException, ClusterNode}
import org.apache.ignite.internal.visor.cache.{VisorCacheLostPartitionsTask, VisorCacheLostPartitionsTaskArg}
import org.apache.ignite.visor.commands.common.VisorTextTable
import org.apache.ignite.visor.visor._
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * ==Overview==
 * Visor 'lost partitions' command implementation.
 *
 * ====Specification====
 * {{{
 *     cache -slp -c=<cache name>
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
 *    cache -slp -c=cache
 *        Show list of lost partitions for cache with name 'cache'.
 * }}}
 */
class VisorCacheLostPartitionsCommand {
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
     * Show list of lost partitions in cache with specified name.
     *
     * ===Examples===
     * <ex>cache -slp -c=cache</ex>
     *     Show list of lost partitions from cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def showLostPartitions(argLst: ArgList, node: Option[ClusterNode]) {
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

        val lostPartitions =
            try
                executeRandom(groupForDataNode(node, cacheName), classOf[VisorCacheLostPartitionsTask],
                    new VisorCacheLostPartitionsTaskArg(Collections.singletonList(cacheName)))
            catch {
                case _: ClusterGroupEmptyException =>
                    scold(messageNodeNotFound(node, cacheName))

                    return
                case e: Throwable =>
                    error(e)

                    return
            }

        if (lostPartitions.getLostPartitions.isEmpty) {
            println(s"""Lost partitions for cache: "${escapeName(cacheName)}" is not found""")

            return
        }

        lostPartitions.getLostPartitions.foreach(cacheLostPartitions => {
            val t = VisorTextTable()

            t #= ("Interval", "Partitions")

            val partitions = cacheLostPartitions._2.toIndexedSeq
            val partitionCnt = partitions.size

            val indexes = mutable.ArrayBuffer.empty[String]
            val partitionRows = mutable.ArrayBuffer.empty[String]
            var startIdx = 0
            var idx = 0
            val b = new StringBuilder

            partitions.foreach((part) => {
                if (idx % 10 == 0)
                    startIdx = part

                b.append(part)
                idx += 1

                if (idx != partitionCnt)
                    b.append(", ")


                if (idx % 10 == 0 || idx == partitionCnt) {
                    indexes += startIdx + "-" + part
                    partitionRows += b.toString().trim
                    b.clear()
                }
            })

            t += (indexes, partitionRows)
            println(s"Lost partitions for cache: ${escapeName(cacheLostPartitions._1)} ($partitionCnt)")
            t.render()
        })
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheLostPartitionsCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheLostPartitionsCommand

    /**
     * Singleton.
     */
    def apply(): VisorCacheLostPartitionsCommand = cmd
}
