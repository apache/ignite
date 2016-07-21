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

package org.apache.ignite.visor.commands.gc

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterGroupEmptyException
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import org.apache.ignite.internal.visor.node.VisorNodeGcTask

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}

/**
 * ==Overview==
 * Contains Visor command `gc` implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------+
 * | gc | Runs garbage collector on remote nodes.                              |
 * |    | If specific node is provided, garbage collector is run on that node. |
 * |    | Otherwise, it will be run on all nodes in topology.                  |
 * +---------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     gc
 *     gc "{-id8=<node-id8>|-id=<node-id>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8=<node-id8>
 *         ID8 of the node.
 *         Note that either '-id8' or '-id' should be specified.
 *         You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.
 *         To specify oldest node on the same host as visor use variable '@nl'.
 *         To specify oldest node on other hosts that are not running visor use variable '@nr'.
 *     -id=<node-id>
 *         ID of the node.
 *         Note that either '-id8' or '-id' can be specified.
 * }}}
 *
 * ====Examples====
 * {{{
 *     gc "-id8=12345678"
 *         Runs garbage collector on specified node.
 *     gc
 *         Runs garbage collector on all nodes in topology.
 * }}}
 */
class VisorGcCommand extends VisorConsoleCommand {
    @impl protected val name = "gc"

    /**
     * ===Command===
     * Runs `System.gc()` on specified node or on all nodes in topology.
     *
     * ===Examples===
     * <ex>gc "-id8=12345678"</ex>
     * Runs `System.gc()` on specified node.
     */
    def gc(args: String) {
        assert(args != null)

        if (isConnected) {
            val argLst = parseArgs(args)

            try {
                val t = VisorTextTable()

                t #= ("Node ID8(@)", "Free Heap Before", "Free Heap After", "Free Heap Delta")

                val NULL: Void = null

                val res = parseNode(argLst) match {
                    case Left(msg) =>
                        scold(msg)

                        return
                    case Right(None) => executeMulti(classOf[VisorNodeGcTask], NULL)
                    case Right(Some(node)) => executeOne(node.id, classOf[VisorNodeGcTask], NULL)
                }

                res.foreach {
                    case (nid, stat) =>
                        val roundHb = stat.get1() / (1024L * 1024L)
                        val roundHa = stat.get2() / (1024L * 1024L)

                        val sign = if (roundHa > roundHb) "+" else ""

                        val deltaPercent = math.round(roundHa * 100d / roundHb - 100)

                        t += (nodeId8(nid), roundHb + "mb", roundHa + "mb", sign + deltaPercent + "%")
                }

                println("Garbage collector procedure results:")

                t.render()
            }
            catch {
                case e: ClusterGroupEmptyException => scold("Topology is empty.")
                case e: IgniteException => scold(e)
            }
        }
        else
            adviseToConnect()
    }

    /**
     * ===Command===
     * Runs `System.gc()` on all nodes in topology.
     *
     * ===Examples===
     * <ex>gc</ex>
     * Runs `System.gc()` on all nodes in topology.
     */
    def gc() {
        gc("")
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorGcCommand {
    /** Singleton command. */
    private val cmd = new VisorGcCommand

    addHelp(
        name = cmd.name,
        shortInfo = "Runs GC on remote nodes.",
        longInfo = List(
            "Runs garbage collector on remote nodes.",
            "If specific node is provided, garbage collector is run on that node.",
            "Otherwise, it will be run on all nodes in topology."
        ),
        spec = List(
            cmd.name,
            s"${cmd.name} {-id8=<node-id8>|-id=<node-id>}"
        ),
        args = List(
            "-id8=<node-id8>" -> List(
                "ID8 of the node.",
                "Note that either '-id8' or '-id' should be specified.",
                "You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.",
                "To specify oldest node on the same host as visor use variable '@nl'.",
                "To specify oldest node on other hosts that are not running visor use variable '@nr'."
            ),
            "-id=<node-id>" -> List(
                "ID of the node.",
                "Note that either '-id8' or '-id' can be specified."
            )
        ),
        examples = List(
            s"${cmd.name} -id8=12345678" ->
                "Runs garbage collector on specified node.",
            cmd.name ->
                "Runs garbage collector on all nodes in topology.",
            s"${cmd.name} -id8=@n0" ->
                "Runs garbage collector on specified node with id8 taken from 'n0' memory variable."
        ),
        emptyArgs = cmd.gc,
        withArgs = cmd.gc
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
    implicit def fromGc2Visor(vs: VisorTag): VisorGcCommand = cmd
}
