/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.events

import java.util.UUID
import collection.immutable._
import collection.JavaConversions._
import org.gridgain.grid._
import org.gridgain.grid.events._
import org.gridgain.grid.events.GridEventType._
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.util.{GridUtils => U}
import org.gridgain.grid.util.scala.impl
import org.gridgain.visor._
import org.gridgain.visor.commands._
import org.gridgain.visor.commands.{VisorConsoleUtils => CU}
import org.gridgain.scalar.scalar._
import visor._
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorCollectEventsTask
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorCollectEventsTask.VisorCollectEventsArgs

/**
 * ==Overview==
 * Visor 'events' commands implementation.
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
 *     events
 *     events "{-id=<node-id>|-id8=<node-id8>} {-e=<ch,cp,de,di,jo,ta,cl,ca,sw>}
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
 *     events "-id8=12345678"
 *         Queries all events from node with '12345678' ID8.
 *     events "-id8=12345678 -e=di,ca"
 *         Queries discovery and cache events from node with '12345678' ID8.
 *     events
 *         Starts command in interactive mode.
 * }}}
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

    private[this] def typeFilter(typeArg: Option[String]) = {
        if (typeArg.isEmpty)
            null
        else {
            val arr = collection.mutable.ArrayBuffer.empty[Int]

            typeArg.get split "," foreach {
                case "ch" => arr ++= EVTS_CHECKPOINT.toList
                case "de" => arr ++= EVTS_DEPLOYMENT.toList
                case "jo" => arr ++= EVTS_JOB_EXECUTION.toList
                case "ta" => arr ++= EVTS_TASK_EXECUTION.toList
                case "ca" => arr ++= EVTS_CACHE.toList
                case "cp" => arr ++= EVTS_CACHE_PRELOAD.toList
                case "sw" => arr ++= EVTS_SWAPSPACE.toList
                case "di" => arr ++= EVTS_DISCOVERY.toList
                case "au" => arr ++= EVTS_AUTHENTICATION.toList
                case t => throw new IllegalArgumentException("Unknown event type: " + t)
            }

            arr.toArray
        }
    }


    private[this] def timeFilter(timeArg: Option[String]): Integer = {
        if (timeArg.isEmpty)
            null
        else {
            val s = timeArg.get

            val n = try
                s.substring(0, s.length - 1).toInt
            catch {
                case _: NumberFormatException =>
                    throw new IllegalArgumentException("Time frame size is not numeric in: " + s)
            }

            if (n <= 0)
                throw new IllegalArgumentException("Time frame size is not positive in: " + s)

            val timeUnit = s.last match {
                case 's' => 1000
                case 'm' => 1000 * 60
                case 'h' => 1000 * 60 * 60
                case 'd' => 1000 * 60 * 60 * 24
                case _ => throw new IllegalArgumentException("Invalid time frame suffix in: " + s)
            }

            n * timeUnit
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
    def events(args: String) {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val typeArg = argValue("e", argLst)
            val timeArg = argValue("t", argLst)

            val id8 = argValue("id8", argLst)
            val id = argValue("id", argLst)

            if (!id8.isDefined && !id.isDefined) {
                scold("Either '-id8' or '-id' must be provided.")

                return
            }

            if (id8.isDefined && id.isDefined) {
                scold("Only one of '-id8' or '-id' is allowed.")

                return
            }

            val node = if (id8.isDefined) {
                val ns = nodeById8(id8.get)

                if (ns.isEmpty) {
                    scold("Unknown 'id8' value: " + id8.get)

                    return
                }

                if (ns.size != 1) {
                    scold("'id8' resolves to more than one node (use full 'id' instead): " + id8.get)

                    return
                }

                ns.head
            }
            else {
                val node = try
                    grid.node(UUID.fromString(id.get))
                catch {
                    case _: IllegalArgumentException =>
                        scold("Invalid node 'id': " + id.get)

                        return
                }

                if (node == null) {
                    scold("'id' does not match any node: " + id.get)

                    return
                }

                node
            }

            val nid = node.id()

            val tpFilter = try
                typeFilter(typeArg)
            catch {
                case e: IllegalArgumentException =>
                    scold(e.getMessage)

                    return
            }

            val tmFilter = try
                timeFilter(timeArg)
            catch {
                case e: IllegalArgumentException =>
                    scold(e.getMessage)

                    return
            }

            val evts = try
                grid.forNode(node).compute().execute(classOf[VisorCollectEventsTask],
                    new VisorCollectEventsArgs(nid, tpFilter, tmFilter)).get match {
                    case x if x.get1() != null =>
                        scold(x.get())

                        return
                    case x if x.get2() == null || x.get2().isEmpty =>
                        println("No events found.")

                        return

                    case x => x.get2()
                }
            catch {
                case e: GridException =>
                    scold(e.getMessage)

                    return
            }

            val sortedOpt = sort(evts, argValue("s", argLst), hasArgName("r", argLst))

            if (!sortedOpt.isDefined)
                return

            val sorted = sortedOpt.get

            val cntOpt = argValue("c", argLst)

            var cnt = Int.MaxValue

            if (cntOpt.isDefined)
                try
                    cnt = cntOpt.get.toInt
                catch {
                    case e: NumberFormatException =>
                        scold("Invalid count: " + cntOpt.get)

                        return
                }

            println("Summary:")

            val st = VisorTextTable()

            st += ("Node ID8(@ID)", nodeId8Addr(nid))
            st += ("Total", sorted.size)
            st += ("Earliest timestamp", formatDateTime(evts.maxBy(_.timestamp).timestamp))
            st += ("Oldest timestamp", formatDateTime(evts.minBy(_.timestamp).timestamp))

            st.render()

            nl()

            println("Per-Event Summary:")

            var sum = Map[Int, (String, Int, Long, Long)]()

            evts.foreach(evt => {
                val info = sum.getOrElse(evt.`type`, (null, 0, Long.MinValue, Long.MaxValue))

                sum += (evt.`type` -> (
                    "(" + evt.mnemonic + ") " + evt.name,
                    info._2 + 1,
                    if (evt.timestamp > info._3) evt.timestamp else info._3,
                    if (evt.timestamp < info._4) evt.timestamp else info._4)
                )
            })

            val et = VisorTextTable()

            et #= (
                "Event",
                "Total",
                ("Earliest/Oldest", "Timestamp"),
                ("Rate", "events/sec")
            )

            sum.values.toList.sortBy(_._2).reverse.foreach(v => {
                val range = v._3 - v._4

                et += (
                    v._1,
                    v._2,
                    (formatDateTime(v._3), formatDateTime(v._4)),
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

    /**
     * Sort events.
     *
     * @param evts Events to sort.
     * @param arg Command argument.
     * @param reverse If `true` sorting is reversed.
     * @return Sorted events.
     */
    private def sort(evts: Array[VisorCollectEventsTask.VisorEventData], arg: Option[String], reverse: Boolean) = {
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

/** Descriptor for `GridEvent`. */
private case class VisorEventData(
    `type`: Int,
    timestamp: Long,
    name: String,
    shortDisplay: String,
    mnemonic: String
)

/**
 * Arguments for `VisorConsoleCollectEventsTask`.
 *
 * @param nodeId Node Id where events should be collected.
 * @param typeArg Arguments for type filter.
 * @param timeArg Arguments for time filter.
 */
private case class VisorConsoleCollectEventsTaskArgs(
    @impl nodeId: UUID,
    typeArg: Option[String],
    timeArg: Option[String]
) extends VisorConsoleOneNodeTaskArgs

/**
 * Task that runs on specified node and returns events data.
 */
@GridInternal
private class VisorConsoleCollectEventsTask
    extends VisorConsoleOneNodeTask[VisorConsoleCollectEventsTaskArgs, Either[Array[VisorEventData], String]] {
    /**
     * Creates predicate that filters events by type.
     *
     * @param types Comma separate list of event type mnemonics.
     * @return Event type filter.
     */
    private def typeFilter(types: Option[String]): Either[EventFilter, String] = {
        if (types.isEmpty)
            Left(_ => true)
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
                case t => return Right("Unknown event type: " + t)
            }

            Left(if (!arr.isEmpty) (e: GridEvent) => arr.contains(e.`type`) else _ => true)
        }
    }

    /**
     * Creates predicate that filters events by timestamp.
     *
     * @param arg Command argument.
     * @return Predicate.
     */
    private def timeFilter(arg: Option[String]): Either[EventFilter, String] = {
        if (arg.isEmpty)
            Left(_ => true)
        else {
            var n = 0

            val s = arg.get

            try
                n = s.substring(0, s.length - 1).toInt
            catch {
                case e: NumberFormatException =>
                    return Right("Time frame size is not numeric in: " + s)
            }

            if (n <= 0) {
                Right("Time frame size is not positive in: " + s)
            }
            else {
                val m = s.last match {
                    case 's' => 1000
                    case 'm' => 1000 * 60
                    case 'h' => 1000 * 60 * 60
                    case 'd' => 1000 * 60 * 60 * 24
                    case _ => 0

                }
                if (m > 0)
                    Left(_.timestamp >= System.currentTimeMillis - n * m)
                else
                    Right("Invalid time frame suffix in: " + s)
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

    protected def run(g: GridEx, arg: VisorConsoleCollectEventsTaskArgs): Either[Array[VisorEventData], String] = {
        typeFilter(arg.typeArg) match {
            case Right(msg) => Right(msg)

            case Left(typeF) => timeFilter(arg.timeArg) match {
                case Right(msg) => Right(msg)
                case Left(timeF) =>
                    val filter = (e: GridEvent) => typeF.apply(e) && timeF.apply(e) && (e match {
                        case te: GridTaskEvent => !CU.containsInTaskName(te.taskName(), te.taskClassName(), "visor")
                        case je: GridJobEvent => !CU.containsInTaskName(je.taskName(), je.taskName(), "visor")
                        case de: GridDeploymentEvent => !de.alias().toLowerCase.contains("visor")
                        case _ => true
                    })

                    Left(g.events().localQuery(filter)
                        .map(e => VisorEventData(e.`type`, e.timestamp(), e.name(), e.shortDisplay(), mnemonic(e)))
                        .toArray)
            }
        }
    }
}


/**
 * Companion object that does initialization of the command.
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
