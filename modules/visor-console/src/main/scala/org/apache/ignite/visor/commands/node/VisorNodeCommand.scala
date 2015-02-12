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

package org.apache.ignite.visor.commands.node

import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.IgniteNodeAttributes._
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.typedef.X
import org.apache.ignite.internal.util.{IgniteUtils => U}

import org.jetbrains.annotations._

import java.util.UUID

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Contains Visor command `node` implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | node | Prints node statistics. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     node "{-id8=<node-id8>|-id=<node-id>} {-a}"
 *     node
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8=<node-id8>
 *         ID8 of node. Either '-id8' or '-id' can be specified.
 *         If neither specified - command starts in interactive mode.
 *     -id=<node-id>
 *         Full ID of node. Either '-id8' or '-id' can  be specified.
 *         If neither specified - command starts in interactive mode.
 *     -a
 *         Print extended information.
 *         By default - only abbreviated statistics is printed.
 * }}}
 *
 * ====Examples====
 * {{{
 *     node
 *         Starts command in interactive mode.
 *     node "-id8=12345678"
 *         Prints statistics for specified node.
 *     node "-id8=12345678 -a"
 *         Prints full statistics for specified node.
 * }}}
 */
class VisorNodeCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help node' to see how to use this command.")
    }

    /**
     * ===Command===
     * Run command in interactive mode.
     *
     * ===Examples===
     * <ex>node</ex>
     * Starts command in interactive mode.
     */
    def node() {
        if (!isConnected)
            adviseToConnect()
        else
            askForNode("Select node from:") match {
                case Some(id) => ask("Detailed statistics (y/n) [n]: ", "n") match {
                    case "n" | "N" => nl(); node("-id=" + id)
                    case "y" | "Y" => nl(); node("-a -id=" + id)
                    case x => nl(); warn("Invalid answer: " + x)
                }
                case None => ()
            }
    }

    /**
     * ===Command===
     * Prints full node information.
     *
     * ===Examples===
     * <ex>node "-id8=12345678"</ex>
     * Prints information for specified node.
     *
     * <ex>node "-id8=12345678 -all"</ex>
     * Prints full information for specified node.
     *
     * @param args Command arguments.
     */
    def node(@Nullable args: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else
            try {
                val argLst = parseArgs(args)

                if (argLst.isEmpty)
                    warn("Missing arguments.").^^
                else {
                    val id8 = argValue("id8", argLst)
                    val id = argValue("id", argLst)
                    val all = hasArgFlag("a", argLst)

                    var node: ClusterNode = null

                    if (id8.isDefined) {
                        val ns = nodeById8(id8.get)

                        if (ns.size != 1)
                            warn("Unknown (invalid) node ID8: " + id8.get).^^
                        else
                            node = ns.head
                    }
                    else if (id.isDefined)
                        try
                            node = ignite.node(UUID.fromString(id.get))
                        catch {
                            case e: IllegalArgumentException => warn("Invalid node ID: " + id.get).^^
                        }
                    else
                        warn("Invalid arguments: " + args).^^

                    if (node != null) {
                        val t = VisorTextTable()

                        t.autoBorder = false

                        t.maxCellWidth = 60

                        t += ("ID", node.id)
                        t += ("ID8", nid8(node))
                        t += ("Order", node.order)

                        (0 /: node.addresses())((b, a) => { t += ("Address (" + b + ")", a); b + 1 })

                        val m = node.metrics

                        val gridName: String = node.attribute(ATTR_GRID_NAME)

                        val ver = U.productVersion(node)
                        val verStr = ver.major() + "." + ver.minor() + "." + ver.maintenance() +
                            (if (F.isEmpty(ver.stage())) "" else "-" + ver.stage())

                        if (all) {
                            t += ("OS info", "" +
                                node.attribute("os.name") + " " +
                                node.attribute("os.arch") + " " +
                                node.attribute("os.version")
                            )
                            t += ("OS user", node.attribute(ATTR_USER_NAME))
                            t += ("Deployment mode", node.attribute(ATTR_DEPLOYMENT_MODE))
                            t += ("Language runtime", node.attribute(ATTR_LANG_RUNTIME))
                            t += ("Ignite version", verStr)
                            t += ("JRE information", node.attribute(ATTR_JIT_NAME))
                            t += ("Non-loopback IPs", node.attribute(ATTR_IPS))
                            t += ("Enabled MACs", node.attribute(ATTR_MACS))
                            t += ("Grid name", safe(gridName, "<default>"))
                            t += ("JVM start time", formatDateTime(m.getStartTime))
                            t += ("Node start time", formatDateTime(m.getNodeStartTime))
                            t += ("Up time", X.timeSpan2HMSM(m.getUpTime))
                            t += ("CPUs", formatNumber(m.getTotalCpus))
                            t += ("Last metric update", formatDateTime(m.getLastUpdateTime))
                            t += ("Maximum active jobs", formatNumber(m.getMaximumActiveJobs))
                            t += ("Current active jobs", formatNumber(m.getCurrentActiveJobs))
                            t += ("Average active jobs", formatDouble(m.getAverageActiveJobs))
                            t += ("Maximum waiting jobs", formatNumber(m.getMaximumWaitingJobs))
                            t += ("Current waiting jobs", formatNumber(m.getCurrentWaitingJobs))
                            t += ("Average waiting jobs", formatDouble(m.getAverageWaitingJobs))
                            t += ("Maximum rejected jobs", formatNumber(m.getMaximumRejectedJobs))
                            t += ("Current rejected jobs", formatNumber(m.getCurrentRejectedJobs))
                            t += ("Average rejected jobs", formatDouble(m.getAverageRejectedJobs))
                            t += ("Maximum cancelled jobs", formatNumber(m.getMaximumCancelledJobs))
                            t += ("Current cancelled jobs", formatNumber(m.getCurrentCancelledJobs))
                            t += ("Average cancelled jobs", formatDouble(m.getAverageCancelledJobs))
                            t += ("Total rejected jobs", formatNumber(m.getTotalRejectedJobs))
                            t += ("Total executed jobs", formatNumber(m.getTotalExecutedJobs))
                            t += ("Total cancelled jobs", formatNumber(m.getTotalCancelledJobs))
                            t += ("Maximum job wait time", formatNumber(m.getMaximumJobWaitTime) + "ms")
                            t += ("Current job wait time", formatNumber(m.getCurrentJobWaitTime) + "ms")
                            t += ("Average job wait time", formatDouble(m.getAverageJobWaitTime) + "ms")
                            t += ("Maximum job execute time", formatNumber(m.getMaximumJobExecuteTime) + "ms")
                            t += ("Curent job execute time", formatNumber(m.getCurrentJobExecuteTime) + "ms")
                            t += ("Average job execute time", formatDouble(m.getAverageJobExecuteTime) + "ms")
                            t += ("Total busy time", formatNumber(m.getTotalBusyTime) + "ms")
                            t += ("Busy time %", formatDouble(m.getBusyTimePercentage * 100) + "%")
                            t += ("Current CPU load %", formatDouble(m.getCurrentCpuLoad * 100) + "%")
                            t += ("Average CPU load %", formatDouble(m.getAverageCpuLoad * 100) + "%")
                            t += ("Heap memory initialized", formatMemory(m.getHeapMemoryInitialized))
                            t += ("Heap memory used", formatMemory(m.getHeapMemoryUsed))
                            t += ("Heap memory committed", formatMemory(m.getHeapMemoryCommitted))
                            t += ("Heap memory maximum", formatMemory(m.getHeapMemoryMaximum))
                            t += ("Non-heap memory initialized", formatMemory(m.getNonHeapMemoryInitialized))
                            t += ("Non-heap memory used", formatMemory(m.getNonHeapMemoryUsed))
                            t += ("Non-heap memory committed", formatMemory(m.getNonHeapMemoryCommitted))
                            t += ("Non-heap memory maximum", formatMemory(m.getNonHeapMemoryMaximum))
                            t += ("Current thread count", formatNumber(m.getCurrentThreadCount))
                            t += ("Maximum thread count", formatNumber(m.getMaximumThreadCount))
                            t += ("Total started thread count", formatNumber(m.getTotalStartedThreadCount))
                            t += ("Current daemon thread count", formatNumber(m.getCurrentDaemonThreadCount))
                        }
                        else {
                            t += ("OS info", "" +
                                node.attribute("os.name") + " " +
                                node.attribute("os.arch") + " " +
                                node.attribute("os.version")
                            )
                            t += ("OS user", node.attribute(ATTR_USER_NAME))
                            t += ("Deployment mode", node.attribute(ATTR_DEPLOYMENT_MODE))
                            t += ("Language runtime", node.attribute(ATTR_LANG_RUNTIME))
                            t += ("Ignite version", verStr)
                            t += ("JRE information", node.attribute(ATTR_JIT_NAME))
                            t += ("Grid name", safe(gridName, "<default>"))
                            t += ("JVM start time", formatDateTime(m.getStartTime))
                            t += ("Node start time", formatDateTime(m.getNodeStartTime))
                            t += ("Up time", X.timeSpan2HMSM(m.getUpTime))
                            t += ("Last metric update", formatDateTime(m.getLastUpdateTime))
                            t += ("CPUs", formatNumber(m.getTotalCpus))
                            t += ("Thread count", formatNumber(m.getCurrentThreadCount))
                            t += ("Cur/avg active jobs", formatNumber(m.getCurrentActiveJobs) +
                                "/" + formatDouble(m.getAverageActiveJobs))
                            t += ("Cur/avg waiting jobs", formatNumber(m.getCurrentWaitingJobs) +
                                "/" + formatDouble(m.getAverageWaitingJobs))
                            t += ("Cur/avg rejected jobs", formatNumber(m.getCurrentRejectedJobs) +
                                "/" + formatDouble(m.getAverageRejectedJobs))
                            t += ("Cur/avg cancelled jobs", formatNumber(m.getCurrentCancelledJobs) +
                                "/" + formatDouble(m.getAverageCancelledJobs))
                            t += ("Cur/avg job wait time", formatNumber(m.getCurrentJobWaitTime) +
                                "/" + formatDouble(m.getAverageJobWaitTime) + "ms")
                            t += ("Cur/avg job execute time", formatNumber(m.getCurrentJobExecuteTime) +
                                "/" + formatDouble(m.getAverageJobExecuteTime) + "ms")
                            t += ("Cur/avg CPU load %", formatDouble(m.getCurrentCpuLoad * 100) +
                                "/" + formatDouble(m.getAverageCpuLoad * 100) + "%")
                            t += ("Heap memory used/max", formatMemory(m.getHeapMemoryUsed) +
                                "/" +  formatMemory(m.getHeapMemoryMaximum))
                        }

                        println("Time of the snapshot: " + formatDateTime(System.currentTimeMillis))

                        t.render()

                        if (!all)
                            println("\nUse \"-a\" flag to see detailed statistics.")
                    }
                }
            }
            catch {
                case e: Exception => scold(e.getMessage)
            }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorNodeCommand {
    // Adds command's help to visor.
    addHelp(
        name = "node",
        shortInfo = "Prints node statistics.",
        spec = List(
            "node {-id8=<node-id8>|-id=<node-id>} {-a}",
            "node"
        ),
        args = List(
            "-id8=<node-id8>" -> List(
                "Note that either '-id8' or '-id' can be specified and " +
                    "you can also use '@n0' ... '@nn' variables as shortcut to <node-id8>.",
                "If neither specified - command starts in interactive mode."
            ),
            "-id=<node-id>" -> List(
                "Full ID of node. Either '-id8' or '-id' can  be specified.",
                "If neither specified - command starts in interactive mode."
            ),
            "-a" -> List(
                "Print extended information.",
                "By default - only abbreviated statistics is printed."
            )
        ),
        examples = List(
            "node" ->
                "Starts command in interactive mode.",
            "node -id8=12345678" ->
                "Prints statistics for specified node.",
            "node -id8=@n0 -a" ->
                "Prints full statistics for specified node with id8 taken from 'n0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.node, cmd.node)
    )

    /** Singleton command. */
    private val cmd = new VisorNodeCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromNode2Visor(vs: VisorTag): VisorNodeCommand = cmd
}
