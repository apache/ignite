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

package org.apache.ignite.visor.commands.disco

import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.events.EventType._
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.internal.visor.event.VisorGridDiscoveryEvent
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask.VisorNodeEventsCollectorTaskArg

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.JavaConversions._
import scala.collection.immutable._
import scala.language.{implicitConversions, reflectiveCalls}

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
 * |       | Note that this command depends on Ignite events.                            |
 * |       |                                                                               |
 * |       | Ignite events can be individually enabled and disabled and disabled events  |
 * |       | can affect the results produced by this command. Note also that configuration |
 * |       | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |       | events on each node can also affect the functionality of this command.        |
 * |       |                                                                               |
 * |       | By default - all events are DISABLED and Ignite stores last 10,000 local     |
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
                val nodes = ignite.nodes()

                if (nodes.isEmpty) {
                    scold("Topology is empty.")

                    return
                }

                val oldest = ignite.nodes().maxBy(_.metrics().getUpTime)

                val cntOpt = argValue("c", argLst)

                val cnt =
                    try
                        cntOpt.fold(Int.MaxValue)(_.toInt)
                    catch {
                        case e: NumberFormatException =>
                            scold("Invalid count: " + cntOpt.get)

                            return
                    }

                println("Oldest alive node in grid: " + nodeId8Addr(oldest.id))

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
                t #= ("Timestamp", "Event", "Node ID8(@)", "IP")

                evts.take(cnt).foreach {
                    case de: VisorGridDiscoveryEvent =>
                        t +=(formatDateTime(de.timestamp()), de.name(),
                            nodeId8(de.evtNodeId()) + (if (de.isDaemon) "(daemon)" else ""),
                            if (F.isEmpty(de.address())) "<n/a>" else de.address())
                    case _ =>
                }

                t.render()

                nl()
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
    private def events(node: ClusterNode, tmFrame: Long, reverse: Boolean) = {
        assert(node != null)
        assert(!node.isDaemon)

        var evts = ignite.compute(ignite.forNode(node)).execute(classOf[VisorNodeEventsCollectorTask],
            toTaskArgument(node.id(), VisorNodeEventsCollectorTaskArg.createEventsArg(EVTS_DISCOVERY, tmFrame))).toSeq

        val nodeStartTime = node.metrics().getStartTime

        if (nodeStartTime > System.currentTimeMillis() - tmFrame) {
            val root = new VisorGridDiscoveryEvent(EVT_NODE_JOINED, null, U.gridEventName(EVT_NODE_JOINED),
                node.id(), nodeStartTime, "", "", node.id, node.addresses().head, node.isDaemon)

            evts = Seq(root) ++ evts
        }

        evts = evts.sortBy(_.timestamp())

        if (reverse) evts.reverse else evts
    }
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
            "Note that this command depends on Ignite events.",
            " ",
            "Ignite events can be individually enabled and disabled and disabled events",
            "can affect the results produced by this command. Note also that configuration",
            "of Event Storage SPI that is responsible for temporary storage of generated",
            "events on each node can also affect the functionality of this command.",
            " ",
            "By default - all events are disabled and Ignite stores last 10,000 local",
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
    implicit def fromDisco2Visor(vs: VisorTag): VisorDiscoveryCommand = cmd
}
