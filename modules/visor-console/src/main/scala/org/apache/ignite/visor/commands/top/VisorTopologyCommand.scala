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

package org.apache.ignite.visor.commands.top

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.IgniteNodeAttributes._
import org.apache.ignite.internal.util.typedef.X
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.lang.IgnitePredicate

import java.net.{InetAddress, UnknownHostException}

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Contains Visor command `top` implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | top | Prints current topology. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     top "{-c1=e1<num> -c2=e2<num> ... -ck=ek<num>} {-h=<host1> ... -h=<hostk>} {-a}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -ck=ek<num>
 *         This defines a mnemonic for node filter:
 *            -cc Number of available CPUs on the node.
 *            -cl Average CPU load (in %) on the node.
 *            -aj Active jobs on the node.
 *            -cj Cancelled jobs on the node.
 *            -tc Thread count on the node.
 *            -it Idle time on the node.
 *                Note: <num> can have 's', 'm', or 'h' suffix indicating
 *                seconds, minutes, and hours. By default (no suffix provided)
 *                value is assumed to be in milliseconds.
 *            -ut Up time on the node.
 *                Note: <num> can have 's', 'm', or 'h' suffix indicating
 *                seconds, minutes, and hours. By default (no suffix provided)
 *                value is assumed to be in milliseconds.
 *            -je Job execute time on the node.
 *            -jw Job wait time on the node.
 *            -wj Waiting jobs count on the node.
 *            -rj Rejected jobs count on the node.
 *            -hu Heap memory used (in MB) on the node.
 *            -hm Heap memory maximum (in MB) on the node.
 *
 *         Comparison part of the mnemonic predicate:
 *            =eq<num> Equal '=' to '<num>' number.
 *            =neq<num> Not equal '!=' to '<num>' number.
 *            =gt<num> Greater than '>' to '<num>' number.
 *            =gte<num> Greater than or equal '>=' to '<num>' number.
 *            =lt<num> Less than '<' to '<num>' number.
 *            =lte<num> Less than or equal '<=' to '<num>' number.
 *     -h=<host>
 *         This defines a host to show nodes from.
 *         Multiple hosts can be provided.
 *     -a
 *         This defines whether to show a separate table of nodes
 *         with detail per-node information.
 * }}}
 *
 * ====Examples====
 * {{{
 *     top "-cc=eq2"
 *         Prints topology for all nodes with two CPUs.
 *     top "-cc=eq2 -a"
 *         Prints full information for all nodes with two CPUs.
 *     top "-h=10.34.2.122 -h=10.65.3.11"
 *         Prints topology for provided hosts.
 *     top
 *         Prints full topology.
 * }}}
 */
class VisorTopologyCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help top' to see how to use this command.")
    }

    /**
     * ===Command===
     * Prints full topology.
     *
     * ===Examples===
     * <ex>top</ex>
     * Prints full topology.
     */
    def top() {
        top("")
    }

    /**
     * ===Command===
     * Prints topology for provided mnemonic predicate.
     *
     * ===Examples===
     * <ex>top "-cc=eq2"</ex>
     * Prints topology for all nodes with two CPUs.
     *
     * <ex>top "-cc=eq2 -a"</ex>
     * Prints full information for all nodes with two CPUs.
     *
     * <ex>top "-h=10.34.2.122 -h=10.65.3.11"</ex>
     * Prints topology for provided hosts.
     *
     * @param args Command arguments.
     */
    def top(args: String) = breakable {
        assert(args != null)

        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val hosts = argLst.filter(_._1 == "h").map((a: Arg) =>
                try
                    InetAddress.getByName(a._2).getHostAddress
                catch {
                    case e: UnknownHostException => scold("Unknown host: " + a._2).^^

                    "" // Never happens.
                }
            ).filter(!_.isEmpty).toSet

            val all = hasArgFlag("a", argLst)

            var f: NodeFilter = (GridNode) => true

            try {
                argLst foreach (arg => {
                    val (n, v) = arg

                    n match {
                        case "cc" if v != null => f = make(v, f, _.metrics.getTotalCpus)
                        case "cl" if v != null => f = make(v, f, (n: ClusterNode) =>
                            (n.metrics.getCurrentCpuLoad * 100).toLong)
                        case "aj" if v != null => f = make(v, f, _.metrics.getCurrentActiveJobs)
                        case "cj" if v != null => f = make(v, f, _.metrics.getCurrentCancelledJobs)
                        case "tc" if v != null => f = make(v, f, _.metrics.getCurrentThreadCount)
                        case "ut" if v != null => f = make(v, f, _.metrics.getUpTime)
                        case "je" if v != null => f = make(v, f, _.metrics.getCurrentJobExecuteTime)
                        case "jw" if v != null => f = make(v, f, _.metrics.getCurrentJobWaitTime)
                        case "wj" if v != null => f = make(v, f, _.metrics.getCurrentWaitingJobs)
                        case "rj" if v != null => f = make(v, f, _.metrics.getCurrentRejectedJobs)
                        case "hu" if v != null => f = make(v, f, _.metrics.getHeapMemoryUsed)
                        case "hm" if v != null => f = make(v, f, _.metrics.getHeapMemoryMaximum)
                        case _ => ()
                    }
                })

                show(n => f(n), hosts, all)
            }
            catch {
                case e: NumberFormatException => scold(e.getMessage)
                case e: IgniteException => scold(e.getMessage)
            }
        }
    }

    /**
     * @param exprStr Expression string.
     * @param f Node filter
     * @param v Value generator.
     */
    private def make(exprStr: String, f: NodeFilter, v: ClusterNode => Long): NodeFilter = {
        assert(exprStr != null)
        assert(f != null)
        assert(v != null)

        val expr = makeExpression(exprStr)

        // Note that if 'f(n)' is false  - 'value' won't be evaluated.
        if (expr.isDefined)
            (n: ClusterNode) => f(n) && expr.get.apply(v(n))
        else
            throw new IgniteException("Invalid expression: " + exprStr)
    }

    /**
     * Prints topology.
     *
     * @param f Node filtering predicate.
     * @param hosts Set of hosts to take nodes from.
     * @param all Whether to show full information.
     */
    private def show(f: NodeFilter, hosts: Set[String], all: Boolean) = breakable {
        assert(f != null)
        assert(hosts != null)

        var nodes = ignite.forPredicate(new IgnitePredicate[ClusterNode] {
            override def apply(e: ClusterNode) = f(e)
        }).nodes()

        if (hosts.nonEmpty)
            nodes = nodes.filter(n => {
                val ips = n.addresses.toSet

                ips.intersect(hosts).nonEmpty
            })

        if (nodes.isEmpty)
            println("Empty topology.").^^

        if (all) {
            val nodesT = VisorTextTable()

            nodesT #= ("Node ID8(@), IP", "Start Time", "Up Time",
                //"Idle Time",
                "CPUs", "CPU Load", "Free Heap")

            nodes foreach ((n: ClusterNode) => {
                val m = n.metrics

                val usdMem = m.getHeapMemoryUsed
                val maxMem = m.getHeapMemoryMaximum

                val freeHeapPct = (maxMem - usdMem) * 100 / maxMem
                val cpuLoadPct = m.getCurrentCpuLoad * 100

                // Add row.
                nodesT += (
                    nodeId8Addr(n.id),
                    formatDateTime(m.getStartTime),
                    X.timeSpan2HMS(m.getUpTime),
                    m.getTotalCpus,
                    safePercent(cpuLoadPct),
                    formatDouble(freeHeapPct) + " %"
                )
            })

            println("Nodes: " +  nodes.size)

            nodesT.render()

            nl()
        }

        val neighborhood = U.neighborhood(nodes)

        val hostsT = VisorTextTable()

        hostsT #= ("Int./Ext. IPs", "Node ID8(@)", "OS", "CPUs", "MACs", "CPU Load")

        neighborhood.foreach {
            case (_, neighbors) =>
                var ips = Set.empty[String]
                var id8s = List.empty[String]
                var macs = Set.empty[String]
                var cpuLoadSum = 0.0

                val n1 = neighbors.head

                assert(n1 != null)

                val cpus = n1.metrics.getTotalCpus
                val os = "" +
                    n1.attribute("os.name") + " " +
                    n1.attribute("os.arch") + " " +
                    n1.attribute("os.version")

                var i = 1

                neighbors.foreach(n => {
                    id8s = id8s :+ (i.toString + ": " + nodeId8(n.id))

                    i += 1

                    ips = ips ++ n.addresses()

                    cpuLoadSum += n.metrics().getCurrentCpuLoad

                    macs = macs ++ n.attribute[String](ATTR_MACS).split(", ").map(_.grouped(2).mkString(":"))
                })

                // Add row.
                hostsT += (
                    ips.toSeq,
                    id8s,
                    os,
                    cpus,
                    macs.toSeq,
                    safePercent(cpuLoadSum / neighbors.size() * 100)
                )
        }

        println("Hosts: " +  neighborhood.size)

        hostsT.render()

        nl()

        val m = ignite.forNodes(nodes).metrics()

        val freeHeap = (m.getHeapMemoryMaximum - m.getHeapMemoryUsed) * 100 /
          m.getHeapMemoryMaximum

        val sumT = VisorTextTable()

        sumT += ("Total hosts", U.neighborhood(nodes).size)
        sumT += ("Total nodes", nodes.size)
        sumT += ("Total CPUs", m.getTotalCpus)
        sumT += ("Avg. CPU load", safePercent(m.getAverageCpuLoad * 100))
        sumT += ("Avg. free heap", formatDouble(freeHeap) + " %")
        sumT += ("Avg. Up time", X.timeSpan2HMS(m.getUpTime))
        sumT += ("Snapshot time", formatDateTime(System.currentTimeMillis))

        println("Summary:")

        sumT.render()
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorTopologyCommand {
    // Adds command's help to visor.
    addHelp(
        name = "top",
        shortInfo = "Prints current topology.",
        spec = List(
            "top {-c1=e1<num> -c2=e2<num> ... -ck=ek<num>} {-h=<host1> ... -h=<hostk>} {-a}"
        ),
        args = List(
            "-ck=ek<num>" -> List(
                "This defines a mnemonic for node filter:",
                "   -cc Number of available CPUs on the node.",
                "   -cl Average CPU load (in %) on the node.",
                "   -aj Active jobs on the node.",
                "   -cj Cancelled jobs on the node.",
                "   -tc Thread count on the node.",
//                "   -it Idle time on the node.",
//                "       Note: <num> can have 's', 'm', or 'h' suffix indicating",
//                "       seconds, minutes, and hours. By default (no suffix provided)",
//                "       value is assumed to be in milliseconds.",
                "   -ut Up time on the node.",
                "       Note: <num> can have 's', 'm', or 'h' suffix indicating",
                "       seconds, minutes, and hours. By default (no suffix provided)",
                "       value is assumed to be in milliseconds.",
                "   -je Job execute time on the node.",
                "   -jw Job wait time on the node.",
                "   -wj Waiting jobs count on the node.",
                "   -rj Rejected jobs count on the node.",
                "   -hu Heap memory used (in MB) on the node.",
                "   -hm Heap memory maximum (in MB) on the node.",
                "",
                "Comparison part of the mnemonic predicate:",
                "   =eq<num> Equal '=' to '<num>' number.",
                "   =neq<num> Not equal '!=' to '<num>' number.",
                "   =gt<num> Greater than '>' to '<num>' number.",
                "   =gte<num> Greater than or equal '>=' to '<num>' number.",
                "   =lt<num> Less than '<' to '<num>' number.",
                "   =lte<num> Less than or equal '<=' to '<num>' number."
            ),
            "-h=<host>" -> List(
                "This defines a host to show nodes from.",
                "Multiple hosts can be provided."
            ),
            "-a" -> List(
                "This defines whether to show a separate table of nodes",
                "with detail per-node information."
            )
        ),
        examples = List(
            "top -cc=eq2" ->
                "Prints topology for all nodes with two CPUs.",
            "top -cc=eq2 -a" ->
                "Prints full information for all nodes with two CPUs.",
            "top -h=10.34.2.122 -h=10.65.3.11" ->
                "Prints topology for provided hosts.",
            "top" ->
                "Prints full topology."
        ),
        ref = VisorConsoleCommand(cmd.top, cmd.top)
    )

    /** Singleton command. */
    private val cmd = new VisorTopologyCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromTop2Visor(vs: VisorTag): VisorTopologyCommand = cmd
}
