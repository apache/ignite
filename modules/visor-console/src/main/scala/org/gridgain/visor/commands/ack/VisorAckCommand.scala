/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.ack

import org.gridgain.grid._
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorAckTask

import java.util.{HashSet => JavaHashSet}

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.gridgain.visor._
import org.gridgain.visor.commands.VisorConsoleCommand
import org.gridgain.visor.visor._
import org.gridgain.grid.util.typedef.T2

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
class VisorAckCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help ack' to see how to use this command.")
    }

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
        if (!isConnected)
            adviseToConnect()
        else
            try {
                val nodeIds = grid.nodes().map(_.id())

                grid.forNodeIds(nodeIds)
                    .compute()
                    .withName("visor-ack")
                    .withNoFailover()
                    .execute(classOf[VisorAckTask], toTaskArgument(nodeIds, msg))
            }
            catch {
                case _: GridEmptyProjectionException => scold("Topology is empty.")
                case e: Exception => scold("System error: " + e.getMessage)
            }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorAckCommand {
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
        ref = VisorConsoleCommand(cmd.ack, cmd.ack)
    )

    /** Singleton command. */
    private val cmd = new VisorAckCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromAck2Visor(vs: VisorTag) = cmd
}
