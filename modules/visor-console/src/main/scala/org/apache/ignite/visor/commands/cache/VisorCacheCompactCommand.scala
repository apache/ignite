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

import org.apache.ignite.internal.visor.cache.VisorCacheCompactTask
import org.apache.ignite.internal.visor.util.VisorTaskUtils._

import org.apache.ignite.cluster.ClusterNode

import java.util.Collections

import org.apache.ignite.visor.commands.VisorTextTable
import org.apache.ignite.visor.visor
import visor._

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'compact' command implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------------------------------+
 * | compact | Compacts all entries in cache on all nodes. |
 * +--------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     compact
 *     compact -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, entries in default cache will be compacted.
 * }}}
 *
 * ====Examples====
 * {{{
 *     compact
 *         Compacts entries in interactively selected cache.
 *     compact -c=cache
 *         Compacts entries in cache with name 'cache'.
 * }}}
 */
class VisorCacheCompactCommand {
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
     * Compacts entries in cache.
     *
     * ===Examples===
     * <ex>cache -compact -c=cache</ex>
     * Compacts entries in cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def compact(argLst: ArgList, node: Option[ClusterNode]) = breakable {
        val cacheArg = argValue("c", argLst)

        val cacheName = cacheArg match {
            case None => null // default cache.

            case Some(s) if s.startsWith("@") =>
                warn("Can't find cache variable with specified name: " + s,
                    "Type 'cache' to see available cache variables."
                )

                break()

            case Some(name) => name
        }

        val prj = if (node.isDefined) ignite.cluster.forNode(node.get) else ignite.cluster.forCacheNodes(cacheName)

        if (prj.nodes().isEmpty) {
            val msg =
                if (cacheName == null)
                    "Can't find nodes with default cache."
                else
                    "Can't find nodes with specified cache: " + cacheName

            scold(msg).^^
        }

        val t = VisorTextTable()

        t #= ("Node ID8(@)", "Entries Compacted", "Cache Size Before", "Cache Size After")

        val cacheSet = Collections.singleton(cacheName)

        prj.nodes().foreach(node => {
            val r = ignite.compute(ignite.cluster.forNode(node))
                .withName("visor-ccompact-task")
                .withNoFailover()
                .execute(classOf[VisorCacheCompactTask], toTaskArgument(node.id(), cacheSet))
                .get(cacheName)

            t += (nodeId8(node.id()), r.get1() - r.get2(), r.get1(), r.get2())
        })

        println("Compacts entries in cache: " + escapeName(cacheName))

        t.render()
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheCompactCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheCompactCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
