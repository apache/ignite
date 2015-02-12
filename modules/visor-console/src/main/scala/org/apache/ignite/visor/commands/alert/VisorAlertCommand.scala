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

package org.apache.ignite.visor.commands.alert

import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.internal.util.lang.{GridFunc => F}

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.events.EventType._
import org.apache.ignite.events.{DiscoveryEvent, Event}
import org.apache.ignite.lang.IgnitePredicate

import java.util.UUID
import java.util.concurrent.atomic._

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor.visor._

import scala.collection.immutable._
import scala.language.implicitConversions
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'alert' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------+
 * | alert | Generates email alerts for user-defined events.             |
 * |       | Node events and grid-wide events are defined via mnemonics. |
 * +---------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     alert
 *     alert "-u {-id=<alert-id>|-a}"
 *     alert "-r {-t=<sec>} {-<metric>=<condition><value>} ... {-<metric>=<condition><value>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -u
 *         Unregisters alert(s). Either '-a' flag or '-id' parameter is required.
 *         Note that only one of the '-u' or '-r' is allowed.
 *         If neither '-u' or '-r' provided - all alerts will be printed.
 *     -a
 *         When provided with '-u' - all alerts will be unregistered.
 *     -id=<alert-id>
 *         When provided with '-u' - alert with matching ID will be unregistered.
 *     -r
 *         Register new alert with mnemonic predicate(s).
 *         Note that only one of the '-u' or '-r' is allowed.
 *         If neither '-u' or '-r' provided - all alerts will be printed.
 *
 *         NOTE: Email settings can be specified in Ignite configu
 *         Email notification will be sent for the alert only
 *         provided mnemonic predicates evaluate to 'true'."
 *     -t
 *         Defines notification frequency in seconds. Default is 15 minutes.
 *         This parameter can only appear with '-r'.
 *     -<metric>
 *         This defines a mnemonic for the metric that will be measured:
 *
 *         Grid-wide metrics (not node specific):
 *            cc - Total number of available CPUs in the grid.
 *            nc - Total number of nodes in the grid.
 *            hc - Total number of physical hosts in the grid.
 *            cl - Current average CPU load (in %) in the grid.
 *
 *         Per-node current metrics:
 *            aj - Active jobs on the node.
 *            cj - Cancelled jobs on the node.
 *            tc - Thread count on the node.
 *            ut - Up time on the node.
 *                Note: <num> can have 's', 'm', or 'h' suffix indicating
 *                seconds, minutes, and hours. By default (no suffix provided)
 *                value is assumed to be in milliseconds.
 *            je - Job execute time on the node.
 *            jw - Job wait time on the node.
 *            wj - Waiting jobs count on the node.
 *            rj - Rejected jobs count on the node.
 *            hu - Heap memory used (in MB) on the node.
 *            cd - Current CPU load on the node.
 *            hm - Heap memory maximum (in MB) on the node.
 *          ),
 *     <condition>
 *        Comparison part of the mnemonic predicate:
 *           eq - Equal '=' to '<value>' number.
 *           neq - Not equal '!=' to '<value>' number.
 *           gt - Greater than '>' to '<value>' number.
 *           gte - Greater than or equal '>=' to '<value>' number.
 *           lt - Less than '<' to 'NN' number.
 *           lte - Less than or equal '<=' to '<value>' number.
 *
 *         NOTE: Email notification will be sent for the alert only when all
 *               provided mnemonic predicates evaluate to 'true'.
 * }}}
 *
 * ====Examples====
 * {{{
 *     alert
 *         Prints all currently registered alerts.
 *     alert "-u -a"
 *         Unregisters all currently registered alerts.
 *     alert "-u -id=12345678"
 *         Unregisters alert with provided ID.
 *     alert "-r -t=900 -cc=gte4 -cl=gt50"
 *         Notify every 15 min if grid has >= 4 CPUs and > 50% CPU load.
 * }}}
 */
class VisorAlertCommand {
    /** Default alert frequency. */
    val DFLT_FREQ = 15L * 60L

    /** Alerts. */
    private var alerts = new HashMap[String, VisorAlert]

    /** Map of last sent notification per alert ID. */
    private var sent = new HashMap[(String, UUID), Long]

    /** Map of alert statistics. */
    private var stats = new HashMap[String, VisorStats]

    /** Last 10 sent alerts. */
    private var last10 = List.empty[VisorSentAlert]

    /** Subscribe guard. */
    private val guard = new AtomicBoolean(false)

    /** Node metric update listener. */
    private var lsnr: IgnitePredicate[Event] = null

    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help alert' to see how to use this command.")
    }

    /**
     * ===Command===
     * Lists all registered alerts.
     *
     * ===Examples===
     * <ex>alert</ex>
     * Prints all currently registered alerts.
     */
    def alert() {
        alert("")
    }

    /**
     * ===Command===
     * Registers, unregisters and list alerts.
     *
     * ===Examples===
     * <ex>alert "-u -a"</ex>
     * Unregisters all currently registered alerts.
     *
     * <ex>alert "-i"</ex>
     * Starts command in interactive mode.
     *
     * <ex>alert "-u -id=12345678"</ex>
     * Unregisters alert with provided ID.
     *
     * <ex>alert "-r -t=900 -cc=gte4 -cl=gt50"</ex>
     * Notify every 15 min if grid has >= 4 CPUs and > 50% CPU load.
     *
     * @param args Command arguments.
     */
    def alert(args: String) {
        assert(args != null)

        val argLst = parseArgs(args)

        if (hasArgFlag("u", argLst))
            unregisterAlert(argLst)
        else if (hasArgFlag("r", argLst))
            registerAlert(argLst)
        else if (args.length() > 0)
            scold("Invalid arguments: " + args)
        else
            printAlerts()
    }

    /**
     * @param exprStr Expression string.
     * @param f Node filter
     * @param value Value generator.
     */
    private def makeNodeFilter(exprStr: String, f: ClusterNode => Boolean, value: ClusterNode => Long):
        ClusterNode => Boolean = {
        assert(exprStr != null)
        assert(f != null)
        assert(value != null)

        val expr = makeExpression(exprStr)

        // Note that if 'f(n)' is false - 'value' won't be evaluated.
        if (expr.isDefined)
            (n: ClusterNode) => f(n) && expr.get.apply(value(n))
        else
            throw new IgniteException("Invalid expression: " + exprStr)
    }

    /**
     * @param exprStr Expression string.
     * @param f Grid filter
     * @param value Value generator.
     */
    private def makeGridFilter(exprStr: String, f: () => Boolean, value: () => Long): () => Boolean = {
        assert(exprStr != null)
        assert(f != null)
        assert(value != null)

        val expr = makeExpression(exprStr)

        // Note that if 'f' is false - 'value' won't be evaluated.
        if (expr.isDefined)
            () => f() && expr.get.apply(value())
        else
            throw new IgniteException("Invalid expression: " + exprStr)
    }

    /**
     * @param args Parsed argument list.
     */
    private def registerAlert(args: ArgList) {
        breakable {
            assert(args != null)

            if (!isConnected)
                adviseToConnect()
            else {
                // Warn but don't halt.
                if (F.isEmpty(ignite.configuration().getAdminEmails))
                    warn("Admin emails are not configured (ignoring).")
                else if (!ignite.isSmtpEnabled)
                    warn("SMTP is not configured (ignoring).")

                val dfltNodeF = (_: ClusterNode) => true
                val dfltGridF = () => true

                var nf = dfltNodeF
                var gf = dfltGridF

                var freq = DFLT_FREQ

                try {
                    args foreach (arg => {
                        val (n, v) = arg

                        n match {
                            // Grid-wide metrics (not node specific).
                            case "cc" if v != null => gf = makeGridFilter(v, gf, ignite.metrics().getTotalCpus)
                            case "nc" if v != null => gf = makeGridFilter(v, gf, ignite.nodes().size)
                            case "hc" if v != null => gf = makeGridFilter(v, gf, U.neighborhood(ignite.nodes()).size)
                            case "cl" if v != null => gf = makeGridFilter(v, gf,
                                () => (ignite.metrics().getAverageCpuLoad * 100).toLong)

                            // Per-node current metrics.
                            case "aj" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentActiveJobs)
                            case "cj" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentCancelledJobs)
                            case "tc" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentThreadCount)
                            case "ut" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getUpTime)
                            case "je" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentJobExecuteTime)
                            case "jw" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentJobWaitTime)
                            case "wj" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentWaitingJobs)
                            case "rj" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getCurrentRejectedJobs)
                            case "hu" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getHeapMemoryUsed)
                            case "hm" if v != null => nf = makeNodeFilter(v, nf, _.metrics().getHeapMemoryMaximum)
                            case "cd" if v != null => nf = makeNodeFilter(v, nf,
                                (n: ClusterNode) => (n.metrics().getCurrentCpuLoad * 100).toLong)

                            // Other tags.
                            case "t" if v != null => freq = v.toLong
                            case "r" => () // Skipping.
                            case _ => throw new IgniteException("Invalid argument: " + makeArg(arg))
                        }
                    })
                }
                catch {
                    case e: NumberFormatException =>
                        scold("Number conversion error: " + e.getMessage)

                        break()

                    case e: Exception =>
                        scold(e.getMessage)

                        break()
                }

                if (nf == null && gf == null) {
                    scold("No predicates have been provided in args: " + makeArgs(args))

                    break()
                }

                val alert = new VisorAlert(
                    id = id8,
                    nodeFilter = nf,
                    gridFilter = gf,
                    perNode = nf != dfltNodeF,
                    perGrid = gf != dfltGridF,
                    spec = makeArgs(args),
                    freq = freq,
                    createdOn = System.currentTimeMillis(),
                    varName = setVar(id8, "a")
                )

                // Subscribe for node metric updates - if needed.
                registerListener()

                alerts = alerts + (alert.id -> alert)
                stats = stats + (alert.id -> VisorStats())

                // Set visor var pointing to created alert.
                mset(alert.varName, alert.id)

                println("Alert '" + alert.id + "' (" + alert.varName + ") registered.")
            }
        }
    }

    /**
     * Registers node metrics update listener, if one wasn't registered already.
     */
    private def registerListener() {
        if (guard.compareAndSet(false, true)) {
            assert(lsnr == null)

            lsnr = new IgnitePredicate[Event] {
                override def apply(evt: Event): Boolean = {
                    val discoEvt = evt.asInstanceOf[DiscoveryEvent]

                    val node = ignite.node(discoEvt.eventNode().id())

                    if (node != null)
                        alerts foreach (t => {
                            val (id, alert) = t

                            var nb = false
                            var gb = false

                            try {
                                nb = alert.nodeFilter(node)
                                gb = alert.gridFilter()
                            }
                            catch {
                                // In case of exception (like an empty projection) - simply return.
                                case _: Throwable => return true
                            }

                            if (nb && gb) {
                                val now = System.currentTimeMillis()

                                val nKey = id -> node.id
                                val gKey = id -> null

                                var go: Boolean = false

                                if (nb && alert.perNode)
                                    go = (now - sent.getOrElse(nKey, 0L)) / 1000 >= alert.freq

                                if (!go && gb && alert.perGrid)
                                    go = (now - sent.getOrElse(gKey, 0L)) / 1000 >= alert.freq

                                if (go) {
                                    // Update throttling.
                                    if (nb && alert.perNode)
                                        sent = sent + (nKey -> now)

                                    if (gb && alert.perGrid)
                                        sent = sent + (gKey -> now)

                                    val stat: VisorStats = stats(id)

                                    assert(stat != null)

                                    // Update stats.
                                    if (stat.firstSnd == 0)
                                        stat.firstSnd = now

                                    stat.cnt += 1
                                    stat.lastSnd = now

                                    stats = stats + (id -> stat)

                                    // Send alert email notification.
                                    sendEmail(alert, if (nb) Some(node) else None)

                                    // Write to Visor log if it is started (see 'log' command).
                                    logText(
                                        "Alert [" +
                                            "id=" + alert.id + "(@" + alert.varName + "), " +
                                            "spec=" + alert.spec + ", " +
                                            "created on=" + formatDateTime(alert.createdOn) +
                                        "]"
                                    )

                                    last10 = VisorSentAlert(
                                        id = alert.id,
                                        spec = alert.spec,
                                        createdOn = alert.createdOn,
                                        sentTs = now
                                    ) +: last10

                                    if (last10.size > 10)
                                        last10 = last10.take(10)
                                }
                            }
                        })

                    true
                }
            }

            ignite.events().localListen(lsnr, EVT_NODE_METRICS_UPDATED)
        }
    }

    /**
     * Unregisters previously registered node metric update listener.
     */
    private def unregisterListener() {
        if (guard.compareAndSet(true, false)) {
            assert(lsnr != null)

            assert(ignite.events().stopLocalListen(lsnr))

            lsnr = null
        }
    }

    /**
     * Resets command.
     */
    private def reset() {
        unregisterAll()
        unregisterListener()
    }

    /**
     * Sends email.
     *
     * @param a Alert to send email about.
     * @param n `Option` for node.
     */
    private def sendEmail(a: VisorAlert, n: Option[ClusterNode]) {
        assert(a != null)
        assert(n != null)

        val subj = "Visor alert triggered: '" + a.spec + '\''
        val headline = "Ignite ver. " + ignite.version()

        val stat = stats(a.id)

        assert(stat != null)

        var body =
            headline + NL +
            NL +
            "----" + NL +
            "Alert ID: " + a.id + NL +
            "Alert spec: "  +  a.spec + NL

        if (n.isDefined)
            body += "Related node ID: " + n.get.id + NL +
                "Related node addresses: " + n.get.addresses() + NL

        body +=
            "Send count: " + stat.cnt + NL +
            "Created on: "  +  formatDateTime(a.createdOn) + NL +
            "First send: " + (if (stat.firstSnd == 0) "n/a" else formatDateTime(stat.firstSnd)) + NL +
            "Last send: " + (if (stat.lastSnd == 0) "n/a" else formatDateTime(stat.lastSnd)) + NL +
            "----" + NL +
            "Grid name: " + ignite.name + NL

        body +=
            "----" + NL +
            NL +
            "NOTE:" + NL +
            "This message is sent by Visor automatically to all configured admin emails." + NL +
            "To change this behavior use 'adminEmails' grid configuration property." + NL +
            NL +
            "| www.gridgain.com" + NL +
            "| support@gridgain.com" + NL

        // Schedule sending.
        ignite.sendAdminEmailAsync(subj, body, false)
    }

    /**
     * Prints advise.
     */
    private def advise() {
        println("\nType 'help alert' to see how to manage alerts.")
    }

    /**
     * Unregisters all alerts.
     */
    private def unregisterAll() {
        mclear("-al")

        alerts = new HashMap[String, VisorAlert]
    }

    /**
     *
     * @param args Parsed argument list.
     */
    private def unregisterAlert(args: ArgList) {
        breakable {
            assert(args != null)

            if (alerts.isEmpty) {
                scold("No alerts have been registered yet.")

                break()
            }

            // Unregister all alerts.
            if (hasArgFlag("a", args)) {
                unregisterAll()

                println("All alerts have been unregistered.")
            }
            // Unregister specific alert.
            else if (hasArgName("id", args)) {
                val idOpt = argValue("id", args)

                if (idOpt.isDefined) {
                    val id = idOpt.get

                    val a = alerts.get(id)

                    if (a.isDefined) {
                        alerts -= id

                        // Clear variable host.
                        mclear(a.get.varName)

                        println("Alert '" + id + "' unregistered.")
                    }
                    else {
                        scold("Failed to find alert with ID: " + id)

                        break()
                    }
                }
                else {
                    scold("No value for '-id' parameter found.")

                    break()
                }
            }
            else {
                scold("Failed to unregister alert.", "Either \"-a\" or \"-id\" parameter is required.")

                break()
            }

            if (alerts.isEmpty)
                unregisterListener()
        }
    }

    /**
     * Prints out all alerts.
     */
    private def printAlerts() {
        if (alerts.isEmpty)
            println("No alerts are registered.")
        else {
            println("Summary:")

            val sum = new VisorTextTable()

            val firstSnd = (-1L /: stats.values)((b, a) => if (b == -1) a.firstSnd else math.min(b, a.firstSnd))
            val lastSnd = (0L /: stats.values)((b, a) => math.max(b, a.lastSnd))

            sum += ("Total alerts", alerts.size)
            sum += ("Total sends", (0 /: stats.values)((b, a) => b + a.cnt))
            sum += ("First send", if (firstSnd == 0) "n/a" else formatDateTime(firstSnd))
            sum += ("Last send", if (lastSnd == 0) "n/a" else formatDateTime(lastSnd))

            sum.render()
        }

        if (last10.isEmpty)
            println("\nNo alerts have been sent.")
        else {
            val last10T = VisorTextTable()

            last10T #= ("ID(@)", "Spec", "Sent", "Registered", "Count")

            last10.foreach((a: VisorSentAlert) => last10T += (
                a.idVar,
                a.spec,
                formatDateTime(a.sentTs),
                formatDateTime(a.createdOn),
                stats(a.id).cnt
            ))

            println("\nLast 10 Triggered Alerts:")

            last10T.render()
        }

        if (alerts.nonEmpty) {
            val tbl = new VisorTextTable()

            tbl #= ("ID(@)", "Spec", "Count", "Registered", "First Send", "Last Send")

            val sorted = alerts.values.toSeq.sortWith(_.varName < _.varName)

            sorted foreach (a => {
                val stat = stats(a.id)

                tbl += (
                    a.id + "(@" + a.varName + ')',
                    a.spec,
                    stat.cnt,
                    formatDateTime(a.createdOn),
                    if (stat.firstSnd == 0) "n/a" else formatDateTime(stat.firstSnd),
                    if (stat.lastSnd == 0) "n/a" else formatDateTime(stat.lastSnd)
                )
            })

            println("\nAlerts: " + sorted.size)

            tbl.render()

            // Print advise.
            advise()
        }
    }

    /**
     * Gets unique ID8 id for the alert.
     *
     * @return 8-character locally unique alert ID.
     */
    private def id8: String = {
        while (true) {
            val id = UUID.randomUUID().toString.substring(0, 8)

            // Have to check for guaranteed uniqueness.
            if (!alerts.contains(id))
                return id
        }

        assert(false, "Should never happen.")

        ""
    }
}

/**
 * Visor alert.
 */
sealed private case class VisorAlert(
    id: String,
    nodeFilter: ClusterNode => Boolean,
    gridFilter: () => Boolean,
    freq: Long,
    spec: String,
    varName: String,
    perNode: Boolean,
    perGrid: Boolean,
    createdOn: Long
) {
    assert(id != null)
    assert(spec != null)
    assert(varName != null)
}

/**
 * Snapshot of the sent alert.
 */
private case class VisorSentAlert(
    id: String,
    sentTs: Long,
    createdOn: Long,
    spec: String
) {
    assert(id != null)
    assert(spec != null)

    def idVar: String = {
        val v = mfind(id)

        if (v.isDefined) id + "(@" + v.get._1 + ")" else id
    }
}

/**
 * Statistics holder for visor alert.
 */
sealed private case class VisorStats(
    var cnt: Int = 0,
    var firstSnd: Long = 0,
    var lastSnd: Long = 0
)

/**
 * Companion object that does initialization of the command.
 */
object VisorAlertCommand {
    addHelp(
        name = "alert",
        shortInfo = "Email alerts for user-defined events.",
        longInfo = Seq(
            "Generates email alerts for user-defined events.",
            "Node events and grid-wide events are defined via mnemonics."
        ),
        spec = Seq(
            "alert",
            "alert -u {-id=<alert-id>|-a}",
            "alert -r {-t=<sec>} {-<metric>=<condition><value>} ... {-<metric>=<condition><value>}"
        ),
        args = Seq(
            "-u" -> Seq(
                "Unregisters alert(s). Either '-a' flag or '-id' parameter is required.",
                "Note that only one of the '-u' or '-r' is allowed.",
                "If neither '-u' or '-r' provided - all alerts will be printed."
            ),
            "-a" ->
                "When provided with '-u' - all alerts will be unregistered.",
            ("-id=<alert-id>",
                "When provided with '-u' - alert with matching ID will be unregistered" +
                "Note you can also use '@a0' ... '@an' variables as shortcut to <alert-id>."),
            "-r" -> Seq(
                "Register new alert with mnemonic predicate(s).",
                "Note that only one of the '-u' or '-r' is allowed.",
                "If neither '-u' or '-r' provided - all alerts will be printed.",
                "",
                "NOTE: Email settings can be specified in Ignite configuration file.",
                "      Email notification will be sent for the alert only when all",
                "      provided mnemonic predicates evaluate to 'true'."
            ),
            "-t" -> Seq(
                "Defines notification frequency in seconds. Default is 15 minutes.",
                "This parameter can only appear with '-r'."
             ),
            "-<metric>" -> Seq(
                "This defines a mnemonic for the metric that will be measured:",
                "",
                "Grid-wide metrics (not node specific):",
                "   cc - Total number of available CPUs in the grid.",
                "   nc - Total number of nodes in the grid.",
                "   hc - Total number of physical hosts in the grid.",
                "   cl - Current average CPU load (in %) in the grid.",
                "",
                "Per-node current metrics:",
                "   aj - Active jobs on the node.",
                "   cj - Cancelled jobs on the node.",
                "   tc - Thread count on the node.",
                "   ut - Up time on the node.",
                "       Note: <num> can have 's', 'm', or 'h' suffix indicating",
                "       seconds, minutes, and hours. By default (no suffix provided)",
                "       value is assumed to be in milliseconds.",
                "   je - Job execute time on the node.",
                "   jw - Job wait time on the node.",
                "   wj - Waiting jobs count on the node.",
                "   rj - Rejected jobs count on the node.",
                "   hu - Heap memory used (in MB) on the node.",
                "   cd - Current CPU load on the node.",
                "   hm - Heap memory maximum (in MB) on the node."
            ),
            "<condition>" -> Seq(
                "Comparison part of the mnemonic predicate:",
                "   eq - Equal '=' to '<value>' number.",
                "   neq - Not equal '!=' to '<value>' number.",
                "   gt - Greater than '>' to '<value>' number.",
                "   gte - Greater than or equal '>=' to '<value>' number.",
                "   lt - Less than '<' to 'NN' number.",
                "   lte - Less than or equal '<=' to '<value>' number."
            )
        ),
        examples = Seq(
            "alert" ->
                "Prints all currently registered alerts.",
            "alert -u -a" ->
                "Unregisters all currently registered alerts.",
            "alert -u -id=12345678" ->
                "Unregisters alert with provided ID.",
            "alert -u -id=@a0" ->
                "Unregisters alert with provided ID taken from '@a0' memory variable.",
            "alert -r -t=900 -cc=gte4 -cl=gt50" ->
                "Notify every 15 min if grid has >= 4 CPUs and > 50% CPU load."
        ),
        ref = VisorConsoleCommand(cmd.alert, cmd.alert)
    )

    /** Singleton command. */
    private val cmd = new VisorAlertCommand

    addCloseCallback(() => {
        cmd.reset()
    })

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromAlert2Visor(vs: VisorTag): VisorAlertCommand = cmd
}
