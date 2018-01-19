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

package org.apache.ignite.visor.commands.ping

import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import java.util.concurrent._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * Ping result container.
 */
private class Result {
    /** Total pings count. */
    var total = 0

    /** Successful pings count. */
    var oks = 0

    /** Failed pings count */
    var fails = 0

    /** Failed nodes. */
    val failedNodes = collection.mutable.Set.empty[ClusterNode]
}

/**
 * Thread that pings one node.
 */
private case class Pinger(n: ClusterNode, res: Result) extends Runnable {
    assert(n != null)
    assert(res != null)

    override def run() {
        val ok = ignite.cluster.pingNode(n.id())

        res.synchronized {
            res.total += 1

            if (ok)
                res.oks += 1
            else {
                res.fails += 1
                res.failedNodes += n
            }
        }
    }
}

/**
 * ==Command==
 * Visor 'ping' command implementation.
 *
 * ==Help==
 * {{{
 * +--------------------+
 * | ping | Pings node. |
 * +--------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     ping {"id81 id82 ... id8k"}
 * }}}
 *
 * ====Arguments====
 * {{{
 *     id8k
 *         ID8 of the node to ping.
 * }}}
 *
 * ====Examples====
 * {{{
 *     ping "12345678"
 *         Pings node with '12345678' ID8.
 *     ping
 *         Pings all nodes in the topology.
 * }}}
 */
class VisorPingCommand extends VisorConsoleCommand {
    @impl protected val name = "ping"

    /**
     * ===Command===
     * Pings node(s) by its ID8.
     *
     * ===Examples===
     * <ex>ping "12345678 56781234"</ex>
     * Pings nodes with '12345678' and '56781234' ID8s.
     *
     * @param args List of node ID8s. If empty or null - pings all nodes in the topology.
     */
    def ping(args: String) = breakable {
        if (checkConnected()) {
            val argLst = parseArgs(args)

            val res = new Result()

            var pings = List.empty[Pinger]

            if (argLst.isEmpty)
                pings ++= ignite.cluster.nodes().map(Pinger(_, res))
            else {
                for (id8 <- argLst) {
                    if (id8._1 != null || id8._2 == null)
                        scold("Invalid ID8: " + argName(id8))
                    else {
                        val ns = nodeById8(id8._2)

                        if (ns.size != 1)
                            scold("Unknown ID8: " + argName(id8))
                        else
                            pings +:= Pinger(ns.head, res)
                    }
                }
            }

            if (pings.isEmpty)
                scold("Topology is empty.")
            else {
                try
                    pings.map(pool.submit(_)).foreach(_.get)
                catch {
                    case _: RejectedExecutionException => scold("Ping failed due to system error.").^^
                }

                val t = VisorTextTable()

                // No synchronization on 'res' is needed since all threads
                // are finished and joined.
                t += ("Total pings", res.total)
                t += ("Successful pings", res.oks + " (" + formatInt(100 * res.oks / res.total) + "%)")
                t += ("Failed pings", res.fails + " (" + formatInt(100 * res.fails / res.total) + "%)")

                if (res.failedNodes.nonEmpty)
                    t += ("Failed nodes", res.failedNodes.map(n => nodeId8Addr(n.id)))

                t.render()
            }
        }
    }

    /**
     * ===Command===
     * Pings all nodes in the topology.
     *
     * ===Examples===
     * <ex>ping</ex>
     * Pings all nodes in the topology.
     */
    def ping() {
        ping("")
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorPingCommand {
    /** Singleton command. */
    private val cmd = new VisorPingCommand

    // Adds command's help to visor.
    addHelp(
        name = cmd.name,
        shortInfo = "Pings node.",
        spec = List(s"${cmd.name} <id81> <id82> ... <id8k>"),
        args = List(
            ("<id8k>",
                "ID8 of the node to ping. Note you can also use '@n0' ... '@nn' variables as shortcut to <id8k>.")
        ),
        examples = List(
            s"${cmd.name} 12345678" ->
                "Pings node with '12345678' ID8.",
            s"${cmd.name} @n0" ->
                "Pings node with 'specified node with ID8 taken from 'n0' memory variable.",
            cmd.name ->
                "Pings all nodes in the topology."
        ),
        emptyArgs = cmd.ping,
        withArgs = cmd.ping
    )

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromPing2Visor(vs: VisorTag): VisorPingCommand = cmd
}
