/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.visor.commands.ack

import org.apache.ignite.cluster.ClusterGroupEmptyException
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.common.VisorConsoleCommand
import org.apache.ignite.visor.visor._
import org.apache.ignite.internal.visor.misc.{VisorAckTask, VisorAckTaskArg}

import scala.language.implicitConversions

/**
 * ==Overview==
 * Visor 'ack' command implementation.
 *
 * ==Help==
 * {{{
 * +-------------------------------------------+
 * | ack | Acks arguments on all remote nodes. |
 * +-------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     ack {"s"}
 *     ack ("s", f)
 * }}}
 *
 * ====Arguments====
 * {{{
 *     s
 *         Optional string to print on each remote node.
 *     f
 *         Optional Scala predicate on 'ScalarRichNodePimp' filtering nodes in the topology.
 * }}}
 *
 * ====Examples====
 * {{{
 *     ack "Howdy!"
 *         Prints 'Howdy!' on all nodes in the topology.
 *     ack("Howdy!", _.id8.startsWith("123"))
 *         Prints 'Howdy!' on all nodes satisfying this predicate.
 *     ack
 *         Prints local node ID on all nodes in the topology.
 * }}}
 */
class VisorAckCommand extends VisorConsoleCommand {
    @impl protected val name = "ack"

    /**
     * ===Command===
     * Acks local node ID on all nodes. Note that this command
     * behaves differently from its sibling that takes an argument.
     *
     * ===Example===
     * <ex>ack</ex>
     * Prints local node IDs on all nodes in the topology.
     */
    def ack() {
        ack(null)
    }

    /**
     * ===Command===
     * Acks its argument on all nodes.
     *
     * ===Example===
     * <ex>ack "Howdy!"</ex>
     * prints 'Howdy!' on all nodes in the topology.
     *
     * @param msg Optional command argument. If `null` this function is no-op.
     */
    def ack(msg: String) {
        if (checkConnected()) {
            try {
                executeMulti(classOf[VisorAckTask], new VisorAckTaskArg(msg))
            }
            catch {
                case _: ClusterGroupEmptyException => scold("Topology is empty.")
                case e: Exception => scold("System error: " + e.getMessage)
            }
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorAckCommand {
    /** Singleton command. */
    private val cmd = new VisorAckCommand

    // Adds command's help to visor.
    addHelp(
        name = "ack",
        shortInfo = "Acks arguments on all remote nodes.",
        spec = Seq(
            "ack",
            "ack <message>"
        ),
        args = Seq(
            "<message>" ->
                "Optional string to print on each remote node."
        ),
        examples = Seq(
            "ack" ->
                "Prints local node ID on all nodes in the topology.",
            "ack Howdy!" ->
                "Prints 'Howdy!' on all nodes in the topology."
        ),
        emptyArgs = cmd.ack,
        withArgs = cmd.ack
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
    implicit def fromAck2Visor(vs: VisorTag): VisorAckCommand = cmd
}
