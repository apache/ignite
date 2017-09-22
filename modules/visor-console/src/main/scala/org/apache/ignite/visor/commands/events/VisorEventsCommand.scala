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

package org.apache.ignite.visor.commands.events

import org.apache.ignite.events.EventType._
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import java.util.UUID

import org.apache.ignite.internal.visor.event.VisorGridEvent
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask.VisorNodeEventsCollectorTaskArg

import scala.collection.JavaConversions._
import scala.collection.immutable._
import scala.language.implicitConversions

/**
 * ==Overview==
 * Visor 'events' commands implementation.
 *
 * ==Help==
 * {{{
 * +----------------------------------------------------------------------------------------+
 * | events | Prints events from a node.                                                    |
 * |        |                                                                               |
 * |        | Note that this command depends on Ignite events.                              |
 * |        |                                                                               |
 * |        | Ignite events can be individually enabled and disabled and disabled events    |
 * |        | can affect the results produced by this command. Note also that configuration |
 * |        | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |        | events on each node can also affect the functionality of this command.        |
 * |        |                                                                               |
 * |        | By default - all events are DISABLED. But if events enabled then Ignite will  |
 * |        | stores last 10,000 local events on each node.                                 |
 * |        | Both of these defaults can be changed in configuration.                       |
 * +----------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     events
 *     events "{-id=<node-id>|-id8=<node-id8>} {-e=<ch,cr,de,di,jo,ta,cl,ca,sw>}
 *         {-t=<num>s|m|h|d} {-s=e|t} {-r} {-c=<n>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8
 *         Node ID8.
 *         Note that either '-id8' or '-id' should be specified.
 *         You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.
 *         To specify oldest node on the same host as visor use variable '@nl'.
 *         To specify oldest node on other hosts that are not running visor use variable '@nr'.
 *         If called without the arguments - starts in interactive mode.
 *     -id=<node-id>
 *         Full node ID.
 *         Either '-id' or '-id8' can be specified.
 *         If called without the arguments - starts in interactive mode.
 *     -e=<ch,de,di,jo,ta,ca,cr,sw>
 *         Comma separated list of event types that should be queried:
 *            ch Checkpoint events.
 *            de Deployment events.
 *            di Discovery events.
 *            jo Job execution events.
 *            ta Task execution events.
 *            cl Cloud events.
 *            ca Cache events.
 *            cr Cache rebalance events.
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
class VisorEventsCommand extends VisorConsoleCommand {
    @impl protected val name: String = "events"

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
     * Gets type filter by mnemonics.
     * @param typeArg Type mnemonics.
     * @throws IllegalArgumentException In case unknown event mnemonic.
     * @return Type id filter.
     */
    @throws[IllegalArgumentException]("In case unknown event mnemonic.")
    protected def typeFilter(typeArg: Option[String]) = {
        typeArg.map(_.split(",").flatMap(typeIds)).orNull
    }

    /**
     * Gets type filter by mnemonic.
     * @param mnemonic Type mnemonic.
     * @throws IllegalArgumentException In case unknown event mnemonic.
     * @return Type id filter.
     */
    @throws[IllegalArgumentException]("In case unknown event mnemonic.")
    protected def typeIds(mnemonic: String) = {
        mnemonic match {
            case "ch" => EVTS_CHECKPOINT
            case "de" => EVTS_DEPLOYMENT
            case "di" => EVTS_DISCOVERY
            case "jo" => EVTS_JOB_EXECUTION
            case "ta" => EVTS_TASK_EXECUTION
            case "ca" => EVTS_CACHE
            case "cr" => EVTS_CACHE_REBALANCE
            case "sw" => EVTS_SWAPSPACE
            case t => throw new IllegalArgumentException("Unknown event mnemonic: " + t)
        }
    }

    /**
     * Gets command's mnemonic for given event.
     *
     * @param e Event to get mnemonic for.
     * @throws IllegalArgumentException In case unknown event type.
     * @return Type mnemonic.
     */
    @throws[IllegalArgumentException]("In case unknown event type.")
    protected def mnemonic(e: VisorGridEvent) = {
        assert(e != null)

        e.typeId() match {
            case t if EVTS_CHECKPOINT.contains(t) => "ch"
            case t if EVTS_DEPLOYMENT.contains(t) => "de"
            case t if EVTS_DISCOVERY_ALL.contains(t) => "di"
            case t if EVTS_JOB_EXECUTION.contains(t)=> "jo"
            case t if EVTS_TASK_EXECUTION.contains(t) => "ta"
            case t if EVTS_CACHE.contains(t) => "ca"
            case t if EVTS_CACHE_REBALANCE.contains(t) => "cr"
            case t if EVTS_SWAPSPACE.contains(t) => "sw"
            case t => throw new IllegalArgumentException("Unknown event type: " + t)
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
        if (isConnected) {
            val argLst = parseArgs(args)

            parseNode(argLst) match {
                case Left(msg) => scold(msg)
                case Right(None) => scold("Either '-id8' or '-id' must be provided.")
                case Right(Some(node)) =>
                    val nid = node.id()

                    val typeArg = argValue("e", argLst)
                    val timeArg = argValue("t", argLst)

                    val evts = try
                        collectEvents(nid, typeArg, timeArg)
                    catch {
                        case e: Exception =>
                            scold(e)

                            return
                    }

                    println("ID8=" + nid8(node))

                    if (evts == null || evts.isEmpty) {
                        println("No events found.")

                        return
                    }

                    val sortedOpt = sort(evts.toList, argValue("s", argLst), hasArgName("r", argLst))

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
                        val info = sum.getOrElse(evt.typeId(), (null, 0, Long.MinValue, Long.MaxValue))

                        sum += (evt.typeId -> (
                            "(" + mnemonic(evt) + ") " + evt.name(),
                            info._2 + 1,
                            if (evt.timestamp() > info._3) evt.timestamp() else info._3,
                            if (evt.timestamp() < info._4) evt.timestamp() else info._4)
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
                        all += (formatDateTime(evt.timestamp()), U.compact(evt.shortDisplay))
                    )

                    all.render()
            }
        }
        else
            adviseToConnect()
    }

    /**
     * Collect events.
     *
     * @param nid Node id.
     * @param typeArg Type ids argument.
     * @param timeArg Time argument.
     * @return
     */
    @throws[Exception]("In case of error.")
    protected def collectEvents(nid: UUID, typeArg: Option[String], timeArg: Option[String]) = {
        val tpFilter = typeFilter(typeArg)

        val tmFilter = timeFilter(timeArg)

        executeOne(nid, classOf[VisorNodeEventsCollectorTask],
            VisorNodeEventsCollectorTaskArg.createEventsArg(tpFilter, tmFilter))
    }

    /**
     * Sort events.
     *
     * @param evts Events to sort.
     * @param arg Command argument.
     * @param reverse If `true` sorting is reversed.
     * @return Sorted events.
     */
    private def sort(evts: List[_ <: VisorGridEvent], arg: Option[String], reverse: Boolean) = {
        assert(evts != null)

        if (arg.isEmpty)
            Some(evts)
        else
            arg.get.trim match {
                case "e" => Some(if (reverse) evts.sortBy(_.name).reverse else evts.sortBy(_.name))
                case "t" => Some(if (reverse) evts.sortBy(_.timestamp).reverse else evts.sortBy(_.timestamp))
                case a: String =>
                    scold("Invalid sorting argument: " + a)

                    None
            }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorEventsCommand {
    /** Singleton command. */
    private val cmd = new VisorEventsCommand

    addHelp(
        name = "events",
        shortInfo = "Print events from a node.",
        longInfo = List(
            "Print events from a node.",
            " ",
            "Note that this command depends on Ignite events.",
            " ",
            "Ignite events can be individually enabled and disabled and disabled events",
            "can affect the results produced by this command. Note also that configuration",
            "of Event Storage SPI that is responsible for temporary storage of generated",
            "events on each node can also affect the functionality of this command.",
            " ",
            "By default - all events are disabled. But if events enabled then Ignite will stores last 10,000 local",
            "events on each node. Both of these defaults can be changed in configuration."
        ),
        spec = List(
            "events",
            "events {-id=<node-id>|-id8=<node-id8>} {-e=<ch,de,di,jo,ta,ca,cr,sw>}",
            "    {-t=<num>s|m|h|d} {-s=e|t} {-r} {-c=<n>}"
        ),
        args = List(
            "-id8=<node-id8>" -> List(
                "Node ID8.",
                "Note that either '-id8' or '-id' should be specified.",
                "You can also use '@n0' ... '@nn' variables as a shortcut for <node-id8>.",
                "To specify oldest node on the same host as visor use variable '@nl'.",
                "To specify oldest node on other hosts that are not running visor use variable '@nr'.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-id=<node-id>" -> List(
                "Full node ID.",
                "Either '-id' or '-id8' can be specified.",
                "If called without the arguments - starts in interactive mode."
            ),
            "-e=<ch,de,di,jo,ta,ca,cr,sw>" -> List(
                "Comma separated list of event types that should be queried:",
                "   ch Checkpoint events.",
                "   de Deployment events.",
                "   di Discovery events.",
                "   jo Job execution events.",
                "   ta Task execution events.",
                "   ca Cache events.",
                "   cr Cache rebalance events.",
                "   sw Swapspace events."
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
        emptyArgs = cmd.events,
        withArgs = cmd.events
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
    implicit def fromEvts2Visor(vs: VisorTag): VisorEventsCommand = cmd
}
