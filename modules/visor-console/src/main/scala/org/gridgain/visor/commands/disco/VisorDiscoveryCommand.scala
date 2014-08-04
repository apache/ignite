/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.disco

import org.gridgain.grid._
import org.gridgain.grid.events._
import org.gridgain.grid.events.GridEventType._
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorEventsCollectTask
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorEventsCollectTask.VisorEventsCollectArgs
import org.gridgain.grid.lang.GridPredicate
import org.gridgain.grid.util.typedef.X

import java.util.UUID

import scala.collection.JavaConversions._
import scala.collection.immutable._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.gridgain.visor.visor._

/**
 * ==Overview==
 * Visor 'disco' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------------------+
 * | disco | Prints topology change log as seen from the oldest node.                      |
 * |       | Timeframe for querying events can be specified in arguments.                   |
 * |       |                                                                               |
 * |       | Note that this command depends on GridGain events.                            |
 * |       |                                                                               |
 * |       | GridGain events can be individually enabled and disabled and disabled events  |
 * |       | can affect the results produced by this command. Note also that configuration |
 * |       | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |       | events on each node can also affect the functionality of this command.        |
 * |       |                                                                               |
 * |       | By default - all events are DISABLED and GridGain stores last 10,000 local     |
 * |       | events on each node. Both of these defaults can be changed in configuration.  |
 * +---------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     disco
 *     disco "{-t=<num>s|m|h|d} {-r} {-c=<n>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -t=<num>s|m|h|d
 *         Defines timeframe for querying events:
 *            =<num>s Events fired during last <num> seconds.
 *            =<num>m Events fired during last <num> minutes.
 *            =<num>h Events fired during last <num> hours.
 *            =<num>d Events fired during last <num> days.
 *     -r
 *         Defines whether sorting should be reversed.
 *     -c=<n>
 *         Defines the maximum events count that can be shown.
 * }}}
 *
 * ====Examples====
 * {{{
 *     disco
 *         Prints all discovery events sorted chronologically (oldest first).
 *     disco "-r"
 *         Prints all discovery events sorted chronologically in reversed order (newest first).
 *     disco "-t=2m"
 *         Prints discovery events fired during last two minutes sorted chronologically.
 * }}}
 */
class VisorDiscoveryCommand {
    /** */
    private type TimeFilter = EventFilter

    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help disco' to see how to use this command.")
    }

    /**
     * ===Command===
     * Prints all discovery events.
     *
     * ===Examples===
     * <ex>disco</ex>
     * Prints all discovery events sorted chronologically (oldest first).
     */
    def disco() {
        disco("")
    }

    /**
     * ===Command===
     * Prints discovery events within specified timeframe.
     *
     * ===Examples===
     * <ex>disco "-r"</ex>
     * Prints all discovery events sorted chronologically in reversed order (newest first).
     *
     * <ex>disco "-t=2m"</ex>
     * Prints discovery events fired during last two minutes.
     */
    def disco(args: String) {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val fs = argValue("t", argLst)

            val tm = if (fs.isDefined) timeFilter(fs) else Long.MaxValue

            if (tm > 0) {
                val nodes = grid.nodes()

                if (nodes.isEmpty)
                    scold("Topology is empty.").^^

                val oldest = grid.nodes().maxBy(_.metrics().getUpTime)

                val cntOpt = argValue("c", argLst)

                val cnt =
                    try
                        cntOpt.map(_.toInt).getOrElse(Int.MaxValue)
                    catch {
                        case e: NumberFormatException =>
                            scold("Invalid count: " + cntOpt.get)

                            return
                    }

                println("Querying oldest node in grid: " + nodeId8Addr(oldest.id))

                val evts =
                    try
                        events(oldest, tm, hasArgFlag("r", argLst))
                    catch {
                        case e: Throwable =>
                            scold(e.getMessage)

                            return
                    }

                if (evts.isEmpty) {
                    scold(
                        "No discovery events found.",
                        "Make sure events are not disabled and Event Storage SPI is properly configured."
                    )

                    return
                }

                nl()

                if (evts.size > cnt)
                    println("Top " + cnt + " Events:")
                else
                    println("All Events:")

                val t = VisorTextTable()

                // Spaces between ID8(@) and IP are intentional!
                t #= ("Event", "Node ID8(@)", "IP", "Timestamp", "Status")

                evts.take(cnt).foreach(e => {
                    t += (
                        e.evtName,
                        nodeId8(e.nodeId),
                        e.ip,
                        formatDateTime(e.ts),
                        status(evts.toList, e)
                    )
                })

                t.render()

                nl()

                println("<root> - current topology root, i.e. oldest alive node.")
                println(  "*      - marks final event for given node and its uptime at that moment.")
            }
        }
    }

    /**
     * Gets chronologically sorted list of discovery events.
     *
     * @param node Node.
     * @param tmFrame Time frame to select events.
     * @param reverse `True` if sort events in reverse order.
     * @return Discovery events.
     */
    private def events(node: GridNode, tmFrame: Long, reverse: Boolean) = {
        assert(node != null)
        assert(!node.isDaemon)

        val nodeIds = grid.nodes().map(_.id())

        val evts = grid.forNode(node).compute().execute(classOf[VisorEventsCollectTask],
            toTaskArgument(nodeIds, VisorEventsCollectArgs.createEventsArg(EVTS_DISCOVERY, tmFrame))).get

        var discoEvts = evts.toSeq.collect {
            case de =>
                val n = grid.node(de.nid())

                VisorDiscoEvent(
                    ts = de.timestamp(),
                    nodeId = de.nid,
                    ip = n.addresses.head,
                    evtName = de.name(),
                    upTime = n.metrics().getUpTime)
        }.toSeq

        // Add made up event for the oldest node since it doesn't have an event about itself.
        discoEvts = discoEvts :+ VisorDiscoEvent(
            ts = node.metrics().getStartTime,
            nodeId = node.id(),
            ip = node.addresses.headOption getOrElse "<n/a>",
            evtName = "<root>",
            upTime = node.metrics.getUpTime)

        if (!reverse) discoEvts.sortBy(_.ts).reverse else discoEvts.sortBy(_.ts)
    }

    /**
     * Returns status of the node.
     *
     * @param evts List of discovery events.
     * @param evt Discovery event for which status is requested.
     * @return Status of the node.
     */
    private def status(evts: List[VisorDiscoEvent], evt: VisorDiscoEvent): String = {
        assert(evts != null)
        assert(evts.nonEmpty)
        assert(evt != null)

        val lastEvt = evts.filter(_.nodeId == evt.nodeId).sortBy(_.ts).last

        val mkUpTime =
            if (evt == lastEvt)
                "* (" + X.timeSpan2HMS(evt.upTime) + ')'
            else
                ""

        lastEvt.evtName match {
            case "NODE_LEFT" => "LEFT" + mkUpTime
            case "NODE_FAILED" => "FAIL" + mkUpTime
            case "NODE_DISCONNECTED" => "DISC" + mkUpTime
            case "NODE_JOINED" | "NODE_RECONNECTED" => "LIVE" + mkUpTime
            case "<root>" => "LIVE" + mkUpTime
            case _ => assert(false, lastEvt.evtName); ""
        }
    }
}

/**
 */
private case class VisorDiscoEvent(
    evtName: String,
    nodeId: UUID,
    ip: String,
    ts: Long,
    upTime: Long
) {
    assert(evtName != null)
    assert(nodeId != null)
    assert(ts > 0)
    assert(ip != null)
    assert(upTime > 0)
}

/**
 * Companion object that does initialization of the command.
 */
object VisorDiscoveryCommand {
    addHelp(
        name = "disco",
        shortInfo = "Prints topology change log.",
        longInfo = List(
            "Prints topology change log as seen from the oldest node.",
            "Timeframe for querying events can be specified in arguments.",
            " ",
            "Note that this command depends on GridGain events.",
            " ",
            "GridGain events can be individually enabled and disabled and disabled events",
            "can affect the results produced by this command. Note also that configuration",
            "of Event Storage SPI that is responsible for temporary storage of generated",
            "events on each node can also affect the functionality of this command.",
            " ",
            "By default - all events are enabled and GridGain stores last 10,000 local",
            "events on each node. Both of these defaults can be changed in configuration."
        ),
        spec = List(
            "disco",
            "disco {-t=<num>s|m|h|d} {-r} {-c=<n>}"
        ),
        args = List(
            "-t=<num>s|m|h|d" -> List(
                "Defines timeframe for quering events:",
                "   =<num>s Events fired during last <num> seconds.",
                "   =<num>m Events fired during last <num> minutes.",
                "   =<num>h Events fired during last <num> hours.",
                "   =<num>d Events fired during last <num> days."
            ),
            "-r" -> List(
                "Defines whether sorting should be reversed."
            ),
            "-c=<n>" -> List(
                "Defines the maximum events count that can be shown."
            )
        ),
        examples = List(
            "disco" ->
                "Prints all discovery events sorted chronologically (oldest first).",
            "disco -r" ->
                "Prints all discovery events sorted chronologically in reversed order (newest first).",
            "disco -t=2m" ->
                "Prints discovery events fired during last two minutes sorted chronologically."
        ),
        ref = VisorConsoleCommand(cmd.disco, cmd.disco)
    )

    /** Singleton command. */
    private val cmd = new VisorDiscoveryCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromDisco2Visor(vs: VisorTag) = cmd
}
