// @scala.file.header

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
import org.gridgain.grid.lang._
import org.gridgain.grid.resources.GridInstanceResource
import org.gridgain.scalar._
import org.gridgain.visor._
import org.gridgain.visor.commands.VisorConsoleCommand
import collection._
import scalar._
import visor._

/**
 * ==Overview==
 * Visor 'ack' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.ack.VisorAckCommand._
 * </ex>
 * Note that `VisorAckCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
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
 *
 * @author @java.author
 * @version @java.version
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
        ack(ALL_NODES_FILTER)
    }

    /**
     * ===Command===
     * Acks local node ID on all nodes. Note that this command
     * behaves differently from its sibling that takes an argument.
     *
     * ===Example===
     * <ex>ack</ex>
     * Prints local node IDs on all nodes in the topology.
     *
     * @param f Optional predicate for filtering out nodes. If `null` - all nodes
     *      will be accepted.
     */
    def ack(f: NodeFilter) {
        if (!isConnected)
            adviseToConnect()
        else
            try
                grid.forPredicate(f)
                    .compute()
                    .withName("visor-ack")
                    .withNoFailover()
                    .run(new VisorAckTask(gg => gg.localNode.id.toString))
            catch {
                case _: GridEmptyProjectionException => scold("Topology is empty.")
                case e: Exception => scold("System error: " + e.getMessage)
            }
    }

    /**
     * ===Command===
     * Acks its argument on all nodes.
     *
     * ===Example===
     * <ex>ack "Howdy!"</ex>
     * prints 'Howdy!' on all nodes in the topology.
     *
     * @param arg Optional command argument. If `null` this function is no-op.
     */
    def ack(arg: String) {
        ack(arg, ALL_NODES_FILTER)
    }

    /**
     * ===Command===
     * Acks its argument on all nodes.
     *
     * ===Example===
     * <ex>ack "Howdy!"</ex>
     * Prints 'Howdy!' on all nodes in the topology.
     *
     * <ex>ack("Howdy!", _.id8.startsWith("123"))"</ex>
     * Prints 'Howdy!' on all nodes satisfying this predicate.
     *
     * @param arg Command argument. If `null` - it's no-op.
     * @param f Optional predicate for filtering out nodes.
     */
    def ack(arg: String, f: NodeFilter) {
        assert(f != null)

        if (!isConnected)
            adviseToConnect()
        else
            try
                grid.forPredicate(f)
                    .compute()
                    .withName("visor-ack")
                    .withNoFailover()
                    .run(new VisorAckTask(_ => arg))
            catch {
                case _: GridEmptyProjectionException => scold("Topology is empty.")
                case e: Exception => scold("System error: " + e.getMessage)
            }
    }
}

/**
 * Companion object that does initialization of the command.
 *
 * @author @java.author
 * @version @java.version
 */
object VisorAckCommand {
    // Adds command's help to visor.
    addHelp(
        name = "ack",
        shortInfo = "Acks arguments on all remote nodes.",
        spec = immutable.Seq(
            "ack",
            "ack <message>"
        ),
        args = immutable.Seq(
            "<message>" ->
                "Optional string to print on each remote node."
        ),
        examples = immutable.Seq(
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

/**
 * Ack task to run on node.
 *
 * @param f - generating message function.
 */
private class VisorAckTask(f: Grid => String) extends GridRunnable with Serializable {
    @GridInstanceResource
    private val gg: Grid = null

    def run() {
        doAck(f(gg))
    }

    /**
     * Ack-ing implementation.
     *
     * @param s String to ack.
     */
    private def doAck(s: Any) {
        println("<visor>: ack: " + s.toString)
    }
}
