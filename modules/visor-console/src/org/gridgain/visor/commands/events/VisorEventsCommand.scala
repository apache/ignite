// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.events

import org.gridgain.scalar.scalar._
import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.gridgain.grid._
import events._
import GridEventType._
import util.{GridUtils => U}
import collection.immutable._
import collection.JavaConversions._
import java.util.UUID
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'events' commands implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.events.VisorEventsCommand._
 * </ex>
 * Note that `VisorEventsCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +----------------------------------------------------------------------------------------+
 * | events | Prints events from a node.                                                     |
 * |        |                                                                               |
 * |        | Note that this command depends on GridGain events.                            |
 * |        |                                                                               |
 * |        | GridGain events can be individually enabled and disabled and disabled events  |
 * |        | can affect the results produced by this command. Note also that configuration |
 * |        | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |        | events on each node can also affect the functionality of this command.        |
 * |        |                                                                               |
 * |        | By default - all events are enabled and GridGain stores last 10,000 local     |
 * |        | events on each node. Both of these defaults can be changed in configuration.  |
 * +----------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor events
 *     visor events "{-id=<node-id>|-id8=<node-id8>} {-e=<ch,cp,de,di,jo,ta,cl,ca,sw>}
 *         {-t=<num>s|m|h|d} {-s=e|t} {-r} {-c=<n>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full node ID.
 *         Either '-id' or '-id8' can be specified.
 *         If called without the arguments - starts in interactive mode.
 *     -id8
 *         Node ID8.
 *         Either '-id' or '-id8' can be specified.
 *         If called without the arguments - starts in interactive mode.
 *     -e=<ch,de,di,jo,ta,cl,ca,sw>
 *         Comma separated list of event types that should be queried:
 *            ch Checkpoint events.
 *            de Deployment events.
 *            di Discovery events.
 *            jo Job execution events.
 *            ta Task execution events.
 *            cl Cloud events.
 *            ca Cache events.
 *            cp Cache pre-loader events.
 *            sw Swapspace events.
 *     -t=<num>s|m|h|d
 *         Defines time frame for querying events:
 *            =<num>s Queries events fired during last <num> seconds.
 *            =<num>m Queries events fired during last <num> minutes.
 *            =<num>h Queries events fired during last <num> hours.
 *            =<num>d Queries events fired during last <num> days.
 *     -s=e|t
 *         Defines sorting of queried events:
 *            =e Sorted by event type.
 *            =t Sorted chronologically.
 *         Only one '=e' or '=t' can be specified.
 *     -r
 *         Defines if sorting should be reversed.
 *         Can be specified only with -s argument.
 *     -c=<n>
 *         Defines the maximum events count that can be shown.
 *         Values in summary tables are calculated over the whole list of events.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor events "-id8=12345678"
 *         Queries all events from node with '12345678' ID8.
 *     visor events "-id8=12345678 -e=di,ca"
 *         Queries discovery and cache events from node with '12345678' ID8.
 *     visor events
 *         Starts command in interactive mode.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
class VisorEventsCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help events' to see how to use this command.")
    }

    /**
     * ===Command===
     * Starts command in interactive mode.
     *
     * ===Examples===
     * <ex>events</ex>
     * Starts command in interactive mode.
     */
    def events() {
        if (!isConnected)
            adviseToConnect()
        else
            askForNode("Select node from:") match {
                case Some(id) => ask("Sort [c]ronologically or by [e]vent type (c/e) [c]: ", "c") match {
                    case "c" | "C" => nl(); events("-s=t -id=" + id)
                    case "e" | "E" => nl(); events("-s=e -id=" + id)
                    case x => nl(); warn("Invalid answer: " + x)
                }
                case None => ()
            }
    }

    /**
     * ===Command===
     * Queries events from specified node filtered by type and/or time frame.
     *
     * ===Examples===
     * <ex>events "-id8=12345678"</ex>
     * Queries all events from node with '12345678' ID8.
     *
     * <ex>events "-id8=12345678 -e=di,ca"</ex>
     * Queries discovery and cache events from node with '12345678' ID8.
     *
     * @param args Command parameters.
     */
    def events(args: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val typeF = typeFilter(argValue("e", argLst))
            val timeF = timeFilter(argValue("t", argLst))

            if (!typeF.isDefined || !timeF.isDefined)
                break()

            val filter = (e: GridEvent) => typeF.get.apply(e) && timeF.get.apply(e) && (e match {
                case evt: GridTaskEvent => !evt.taskName().toLowerCase.contains("visor")
                case evt: GridJobEvent => !evt.taskName().toLowerCase.contains("visor")
                case evt: GridDeploymentEvent => !evt.alias().toLowerCase.contains("visor")
                case _ => true
            })

            val id8 = argValue("id8", argLst)
            val id = argValue("id", argLst)

            var evts: collection.Iterable[GridEvent] = null

            var nid: UUID = null

            try
                if (!id8.isDefined && !id.isDefined)
                    scold("Either '-id8' or '-id' must be provided.").^^
                else if (id8.isDefined && id.isDefined)
                    scold("Only one of '-id8' or '-id' is allowed.").^^
                else if (id8.isDefined) {
                    val ns = nodeById8(id8.get)

                    if (ns.isEmpty)
                        scold("Unknown 'id8' value: " + id8.get).^^
                    else if (ns.size != 1)
                        scold("'id8' resolves to more than one node (use full 'id' instead): " + id8.get).^^
                    else {
                        val head = ns.head

                        nid = head.id

                        evts = grid.forNode(head).events().remoteQuery(filter, 0).get
                    }
                }
                else {
                    assert(id.isDefined)

                    try {
                        val node = grid.node(UUID.fromString(id.get))

                        if (node == null)
                            scold("'id' does not match any node: " + id.get).^^

                        nid = node.id

                        evts = grid.forNode(node).events().remoteQuery(filter, 0).get
                    }
                    catch {
                        case e: IllegalArgumentException => scold("Invalid node 'id': " + id.get).^^
                    }
                }
            catch {
                case e: GridException => scold(e.getMessage).^^
            }

            assert(evts != null)
            assert(nid != null)

            if (evts.isEmpty)
                println("No events found.")
            else {
                val sortedOpt = sort(evts, argValue("s", argLst), hasArgName("r", argLst))

                if (!sortedOpt.isDefined)
                    break()

                val sorted = sortedOpt.get

                val cntOpt = argValue("c", argLst)

                var cnt = Int.MaxValue

                if (cntOpt.isDefined)
                    try
                        cnt = cntOpt.get.toInt
                    catch {
                        case e: NumberFormatException => scold("Invalid count: " + cntOpt.get).^^
                    }

                println("Summary:")

                val st = VisorTextTable()

                st += ("Node ID8(@ID)", nodeId8Addr(nid))
                st += ("Total", sorted.size)
                st += ("Earliest timestamp",
                    formatDateTime(evts.max(Ordering.by[GridEvent, Long](_.timestamp)).timestamp))
                st += ("Oldest timestamp",
                    formatDateTime(evts.min(Ordering.by[GridEvent, Long](_.timestamp)).timestamp))

                st.render()

                nl()

                println("Per-Event Summary:")

                var sum = Map[Int, (String, Int, Long, Long)]()

                evts.foreach(evt => {
                    val info = sum.getOrElse(evt.`type`, (null, 0, Long.MinValue, Long.MaxValue))

                    sum += (evt.`type` -> (
                        "(" + mnemonic(evt) + ") " + evt.name,
                        info._2 + 1,
                        if (evt.timestamp > info._3) evt.timestamp else info._3,
                        if (evt.timestamp < info._4) evt.timestamp else info._4)
                    )
                })

                val et = VisorTextTable()

                et #= (
                    "Event",
                    "Total",
                    (
                        "Earliest/Oldest",
                        "Timestamp"
                    ),
                    (
                        "Rate",
                        "events/sec"
                    )
                )

                sum.values.toList.sortBy(_._2).reverse.foreach(v => {
                    val range = v._3 - v._4

                    et += (
                        v._1,
                        v._2,
                        (
                            formatDateTime(v._3),
                            formatDateTime(v._4)
                        ),
                        formatDouble(if (range != 0) (v._2.toDouble * 1000) / range else v._2)
                    )
                })

                et.render()

                nl()

                if (sorted.size > cnt)
                    println("Top " + cnt + " Events:")
                else
                    println("All Events:")

                val all = VisorTextTable()

                all.maxCellWidth = 50

                all #= ("Timestamp", "Description")

                sorted.take(cnt).foreach(evt =>
                    all += (formatDateTime(evt.timestamp), U.compact(evt.shortDisplay))
                )

                all.render()
            }
        }
    }

    /**
     * Gets command's mnemonic for given event.
     *
     * @param e Event to get mnemonic for.
     */
    private def mnemonic(e: GridEvent): String = {
        assert(e != null)

        e match {
            case di: GridDiscoveryEvent => "di"
            case ch: GridCheckpointEvent => "ch"
            case de: GridDeploymentEvent => "de"
            case jo: GridJobEvent => "jo"
            case ta: GridTaskEvent => "ta"
            case ca: GridCacheEvent => "ca"
            case sw: GridSwapSpaceEvent => "sw"
            case cp: GridCachePreloadingEvent => "cp"
            case au: GridAuthenticationEvent => "au"

            // Should never happen.
            case _ => throw new GridRuntimeException("Unknown event type: " + e)
        }
    }

    /**
     * Creates predicate that filters events by type.
     *
     * @param types Comma separate list of event type mnemonics.
     * @return Event type filter.
     */
    private def typeFilter(types: Option[String]): Option[EventFilter] = {
        if (types.isEmpty)
            Some(_ => true)
        else {
            var arr: List[Int] = Nil

            types.get split "," foreach {
                case "ch" => arr ++= EVTS_CHECKPOINT.toList
                case "de" => arr ++= EVTS_DEPLOYMENT.toList
                case "jo" => arr ++= EVTS_JOB_EXECUTION.toList
                case "ta" => arr ++= EVTS_TASK_EXECUTION.toList
                case "ca" => arr ++= EVTS_CACHE.toList
                case "cp" => arr ++= EVTS_CACHE_PRELOAD.toList
                case "sw" => arr ++= EVTS_SWAPSPACE.toList
                case "di" => arr ++= EVTS_DISCOVERY.toList
                case "au" => arr ++= EVTS_AUTHENTICATION.toList

                case t =>
                    scold("Unknown event type: " + t)

                    return None
            }

            Some(if (!arr.isEmpty) (e: GridEvent) => arr.contains(e.`type`) else _ => true)
        }
    }

    /**
     * Creates predicate that filters events by timestamp.
     *
     * @param arg Command argument.
     * @return Predicate.
     */
    private def timeFilter(arg: Option[String]): Option[EventFilter] = {
        if (arg.isEmpty)
            Some(_ => true)
        else {
            var n = 0

            val s = arg.get

            try
                n = s.substring(0, s.length - 1).toInt
            catch {
                case e: NumberFormatException =>
                    scold("Time frame size is not numeric in: " + s)

                    return None
            }

            if (n <= 0) {
                scold("Time frame size is not positive in: " + s)

                None
            }
            else {
                val m = s.last match {
                    case 's' => 1000
                    case 'm' => 1000 * 60
                    case 'h' => 1000 * 60 * 60
                    case 'd' => 1000 * 60 * 60 * 24
                    case _ =>
                        scold("Invalid time frame suffix in: " + s)

                        return None
                }

                Some(_.timestamp >= System.currentTimeMillis - n * m)
            }
        }
    }

    /**
     * Sort events.
     *
     * @param evts Events to sort.
     * @param arg Command argument.
     * @param reverse If `true` sorting is reversed.
     * @return Sorted events.
     */
    private def sort(evts: collection.Iterable[GridEvent], arg: Option[String], reverse: Boolean):
        Option[List[GridEvent]] = {
        assert(evts != null)

        val list = evts.toList

        if (arg.isEmpty)
            Some(list)
        else
            arg.get.trim match {
                case "e" => Some(if (reverse) list.sortBy(_.name).reverse else list.sortBy(_.name))
                case "t" => Some(if (reverse) list.sortBy(_.timestamp).reverse else list.sortBy(_.timestamp))
                case a: String =>
                    scold("Invalid sorting argument: " + a)

                    None
            }
    }
}

/**
 * Companion object that does initialization of the command.
 *
 * @author @java.author
 * @version @java.version
 */
object VisorEventsCommand {
    addHelp(
        name = "events",
        shortInfo = "Print events from a node.",
        longInfo = List(
            "Print events from a node.",
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
            "events",
            "events {-id=<node-id>|-id8=<node-id8>} {-e=<ch,cp,de,di,jo,ta,cl,ca,sw,au>}",
            "    {-t=<num>s|m|h|d} {-s=e|t} {-r} {-c=<n>}"
        ),
        args = List(
            "-id=<node-id>" -> List(
                "Full node ID.",
                "Either '-id' or '-id8' can be specified.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-id8=<node-id8>" -> List(
                "Node ID8.",
                "Note that either '-id8' or '-id' can be specified and " +
                    "you can also use '@n0' ... '@nn' variables as shortcut to <node-id8>.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-e=<ch,de,di,jo,ta,cl,ca,sw>" -> List(
                "Comma separated list of event types that should be queried:",
                "   ch Checkpoint events.",
                "   de Deployment events.",
                "   di Discovery events.",
                "   jo Job execution events.",
                "   ta Task execution events.",
                "   ca Cache events.",
                "   cp Cache pre-loader events.",
                "   sw Swapspace events.",
                "   au Authentication events."
            ),
            "-t=<num>s|m|h|d" -> List(
                "Defines time frame for quering events:",
                "   =<num>s Queries events fired during last <num> seconds.",
                "   =<num>m Queries events fired during last <num> minutes.",
                "   =<num>h Queries events fired during last <num> hours.",
                "   =<num>d Queries events fired during last <num> days."
            ),
            "-s=e|t" -> List(
                "Defines sorting of queried events:",
                "   =e Sorted by event type.",
                "   =t Sorted chronologically.",
                "Only one '=e' or '=t' can be specified."
            ),
            "-r" -> List(
                "Defines if sorting should be reversed.",
                "Can be specified only with -s argument."
            ),
            "-c=<n>" -> List(
                "Defines the maximum events count that can be shown.",
                "Values in summary tables are calculated over the whole list of events."
            )
        ),
        examples = List(
            "events -id8=12345678" ->
                "Queries all events from node with '12345678' id8.",
            "events -id8=@n0" ->
                "Queries all events from node with id8 taken from 'n0' memory variable.",
            "events -id8=12345678 -e=di,ca" ->
                "Queries discovery and cache events from node with '12345678' ID8.",
            "events" ->
                "Starts command in interactive mode."
        ),
        ref = VisorConsoleCommand(cmd.events, cmd.events)
    )

    /** Singleton command. */
    private val cmd = new VisorEventsCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromEvts2Visor(vs: VisorTag) = cmd
}
