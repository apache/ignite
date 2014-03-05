/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.node

import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.jetbrains.annotations._
import org.gridgain.grid._
import org.gridgain.grid.util.typedef._
import kernal.GridNodeAttributes._
import java.util.UUID
import java.text._
import scala.util.control.Breaks._
import scala.collection.immutable._
import scala.collection.JavaConversions._
import org.gridgain.scalar._
import scalar._

/**
 * ==Overview==
 * Contains Visor command `node` implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.node.VisorNodeCommand._
 * </ex>
 * Note that `VisorNodeCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
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
 *     node "{id8=<node-id8>|id=<node-id>} {-a}"
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
    /** */
    private val KB = 1024

    /** */
    private val MB = KB * 1024

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
                    warn("Missing arguments.") ^^
                else {
                    val id8 = argValue("id8", argLst)
                    val id = argValue("id", argLst)
                    val all = hasArgFlag("a", argLst)

                    var node: GridNode = null

                    if (id8.isDefined) {
                        val ns = nodeById8(id8.get)

                        if (ns.size != 1)
                            warn("Unknown (invalid) node ID8: " + id8.get) ^^
                        else
                            node = ns.head
                    }
                    else if (id.isDefined)
                        try
                            node = grid.node(UUID.fromString(id.get))
                        catch {
                            case e: IllegalArgumentException => warn("Invalid node ID: " + id.get) ^^
                        }
                    else
                        warn("Invalid arguments: " + args) ^^

                    if (node != null) {
                        val t = VisorTextTable()

                        t += ("ID", node.id)
                        t += ("ID8", nid8(node))
                        t += ("Order", node.order)

                        (0 /: node.addresses())((b, a) => { t += ("Address (" + b + ")", a); b + 1 })

                        val m = node.metrics

                        val nmFmt = new DecimalFormat("#")
                        val kbFmt = new DecimalFormat("###,###,###,###,###")

                        val gridName: String = node.attribute(ATTR_GRID_NAME)

                        if (all) {
                            t += ("OS info", "" +
                                node.attribute("os.name") + " " +
                                node.attribute("os.arch") + " " +
                                node.attribute("os.version")
                            )
                            t += ("OS user", node.attribute(ATTR_USER_NAME))
                            t += ("Deployment mode", node.attribute(ATTR_DEPLOYMENT_MODE))
                            t += ("Language runtime", node.attribute(ATTR_LANG_RUNTIME))
                            t += ("GridGain build#", node.attribute(ATTR_BUILD_VER))
                            t += ("JRE information", node.attribute(ATTR_JIT_NAME))
                            t += ("Non-loopback IPs", node.attribute(ATTR_IPS))
                            t += ("Enabled MACs", node.attribute(ATTR_MACS))
                            t += ("Grid name", safe(gridName, "<default>"))
                            t += ("JVM start time", formatDateTime(m.getStartTime))
                            t += ("Node start time", formatDateTime(m.getNodeStartTime))
                            t += ("Up time", X.timeSpan2HMSM(m.getUpTime))
                            t += ("CPUs", nmFmt.format(m.getTotalCpus))
                            t += ("Last metric update", formatDateTime(m.getLastUpdateTime))
                            t += ("Maximum active jobs", nmFmt.format(m.getMaximumActiveJobs))
                            t += ("Current active jobs", nmFmt.format(m.getCurrentActiveJobs))
                            t += ("Average active jobs", formatDouble(m.getAverageActiveJobs))
                            t += ("Maximum waiting jobs", nmFmt.format(m.getMaximumWaitingJobs))
                            t += ("Current waiting jobs", nmFmt.format(m.getCurrentWaitingJobs))
                            t += ("Average waiting jobs", formatDouble(m.getAverageWaitingJobs))
                            t += ("Maximum rejected jobs", nmFmt.format(m.getMaximumRejectedJobs))
                            t += ("Current rejected jobs", nmFmt.format(m.getCurrentRejectedJobs))
                            t += ("Average rejected jobs", formatDouble(m.getAverageRejectedJobs))
                            t += ("Maximum cancelled jobs", nmFmt.format(m.getMaximumCancelledJobs))
                            t += ("Current cancelled jobs", nmFmt.format(m.getCurrentCancelledJobs))
                            t += ("Average cancelled jobs", formatDouble(m.getAverageCancelledJobs))
                            t += ("Total rejected jobs", nmFmt.format(m.getTotalRejectedJobs))
                            t += ("Total executed jobs", nmFmt.format(m.getTotalExecutedJobs))
                            t += ("Total cancelled jobs", nmFmt.format(m.getTotalCancelledJobs))
                            t += ("Maximum job wait time", nmFmt.format(m.getMaximumJobWaitTime) + "ms")
                            t += ("Current job wait time", nmFmt.format(m.getCurrentJobWaitTime) + "ms")
                            t += ("Average job wait time", formatDouble(m.getAverageJobWaitTime) + "ms")
                            t += ("Maximum job execute time", nmFmt.format(m.getMaximumJobExecuteTime) + "ms")
                            t += ("Curent job execute time", nmFmt.format(m.getCurrentJobExecuteTime) + "ms")
                            t += ("Average job execute time", formatDouble(m.getAverageJobExecuteTime) + "ms")
                            t += ("Total busy time", nmFmt.format(m.getTotalBusyTime) + "ms")
                            t += ("Busy time %", formatDouble(m.getBusyTimePercentage * 100) + "%")
                            t += ("Current CPU load %", formatDouble(m.getCurrentCpuLoad * 100) + "%")
                            t += ("Average CPU load %", formatDouble(m.getAverageCpuLoad * 100) + "%")
                            t += ("Heap memory initialized", kbFmt.format(m.getHeapMemoryInitialized / MB) + "mb")
                            t += ("Heap memory used", kbFmt.format(m.getHeapMemoryUsed / MB) + "mb")
                            t += ("Heap memory committed", kbFmt.format(m.getHeapMemoryCommitted / MB) + "mb")
                            t += ("Heap memory maximum", kbFmt.format(m.getHeapMemoryMaximum / MB) + "mb")
                            t += ("Non-heap memory initialized", kbFmt.format(m.getNonHeapMemoryInitialized / MB) + "mb")
                            t += ("Non-heap memory used", kbFmt.format(m.getNonHeapMemoryUsed / MB) + "mb")
                            t += ("Non-heap memory committed", kbFmt.format(m.getNonHeapMemoryCommitted / MB) + "mb")
                            t += ("Non-heap memory maximum", kbFmt.format(m.getNonHeapMemoryMaximum / MB) + "mb")
                            t += ("Current thread count", nmFmt.format(m.getCurrentThreadCount))
                            t += ("Maximum thread count", nmFmt.format(m.getMaximumThreadCount))
                            t += ("Total started thread count", nmFmt.format(m.getTotalStartedThreadCount))
                            t += ("Current daemon thread count", nmFmt.format(m.getCurrentDaemonThreadCount))
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
                            t += ("GridGain build#", node.attribute(ATTR_BUILD_VER))
                            t += ("JRE information", node.attribute(ATTR_JIT_NAME))
                            t += ("Grid name", safe(gridName, "<default>"))
                            t += ("JVM start time", formatDateTime(m.getStartTime))
                            t += ("Node start time", formatDateTime(m.getNodeStartTime))
                            t += ("Up time", X.timeSpan2HMSM(m.getUpTime))
                            t += ("Last metric update", formatDateTime(m.getLastUpdateTime))
                            t += ("CPUs", nmFmt.format(m.getTotalCpus))
                            t += ("Thread count", nmFmt.format(m.getCurrentThreadCount))
                            t += ("Cur/avg active jobs", nmFmt.format(m.getCurrentActiveJobs) +
                                "/" + formatDouble(m.getAverageActiveJobs))
                            t += ("Cur/avg waiting jobs", nmFmt.format(m.getCurrentWaitingJobs) +
                                "/" + formatDouble(m.getAverageWaitingJobs))
                            t += ("Cur/avg rejected jobs", nmFmt.format(m.getCurrentRejectedJobs) +
                                "/" + formatDouble(m.getAverageRejectedJobs))
                            t += ("Cur/avg cancelled jobs", nmFmt.format(m.getCurrentCancelledJobs) +
                                "/" + formatDouble(m.getAverageCancelledJobs))
                            t += ("Cur/avg job wait time", nmFmt.format(m.getCurrentJobWaitTime) +
                                "/" + formatDouble(m.getAverageJobWaitTime) + "ms")
                            t += ("Cur/avg job execute time", nmFmt.format(m.getCurrentJobExecuteTime) +
                                "/" + formatDouble(m.getAverageJobExecuteTime) + "ms")
                            t += ("Cur/avg CPU load %", formatDouble(m.getCurrentCpuLoad * 100) +
                                "/" + formatDouble(m.getAverageCpuLoad * 100) + "%")
                            t += ("Heap memory used/max", kbFmt.format(m.getHeapMemoryUsed / MB) +
                                "/" +  kbFmt.format(m.getHeapMemoryMaximum / MB) + "mb")
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
            "node {id8=<node-id8>|id=<node-id>} {-a}",
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
    implicit def fromNode2Visor(vs: VisorTag) = cmd
}
