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

import java.io.{File, FileNotFoundException}
import java.util.UUID
import java.util.concurrent.atomic._

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.events.EventType._
import org.apache.ignite.events.{DiscoveryEvent, Event}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.lang.IgnitePredicate
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.alert.VisorAlertCommand._
import org.apache.ignite.visor.commands.common.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite.visor.visor._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.language.implicitConversions
import scala.sys.process.{Process, ProcessLogger}
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'alert' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------+
 * | alert | Generates alerts for user-defined events.                   |
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
 *     -n
 *         Alert name
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
 *     -t
 *         Defines notification frequency in seconds. Default is 60 seconds.
 *         This parameter can only appear with '-r'.
 *     -s
 *         Define script for execution when alert triggered.
 *         For configuration of throttle period see -i argument.
 *         Script will receive following arguments:
 *             1) Alert name or alert ID when name is not defined.
 *             2) Alert condition as string.
 *             3, ...) Values of alert conditions ordered as in alert command.
 *     -i
 *         Configure alert notification minimal throttling interval in seconds. Default is 60 seconds.
 *
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
 *     alert "-r -n=Nodes -t=15 -nc=gte3 -s=/home/user/scripts/alert.sh -i=300"
 *         Notify every 15 second if grid has >= 3 nodes and execute script "/home/user/scripts/alert.sh" with
 *         repeat interval not less than 5 min.
 * }}}
 */
class VisorAlertCommand extends VisorConsoleCommand {
    @impl protected val name = "alert"

    /** Alerts. */
    private var alerts = new HashMap[String, VisorAlert]

    /** Map of last sent notification per alert ID. */
    private var sent = new HashMap[String, Long]

    /** Map of alert statistics. */
    private var stats = new HashMap[String, VisorStats]

    /** Last 10 sent alerts. */
    private var last10 = List.empty[VisorSentAlert]

    /** Subscribe guard. */
    private val guard = new AtomicBoolean(false)

    /** Node metric update listener. */
    private var lsnr: IgnitePredicate[Event] = null

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
     * Create function to check specific node condition.
     *
     * @param exprStr Expression string.
     * @param value Value generator.
     */
    private def makeNodeFilter(exprStr: String, value: (ClusterNode) => Long): Option[(ClusterNode) => (Long, Boolean)] = {
        assert(exprStr != null)
        assert(value != null)

        val expr = makeExpression(exprStr)

        // Note that if 'f(n)' is false - 'value' won't be evaluated.
        expr match {
            case Some(f) => Some((n: ClusterNode) => {
                val v = value(n)

                (v, f.apply(v))
            })

            case _ => throw new IgniteException("Invalid expression: " + exprStr)
        }
    }

    /**
     * Create function to check specific grid condition.
     *
     * @param exprStr Expression string.
     * @param value Value generator.
     */
    private def makeGridFilter(exprStr: String, value: () => Long): Option[() => (Long, Boolean)] = {
        assert(exprStr != null)
        assert(value != null)

        val expr = makeExpression(exprStr)

        // Note that if 'f' is false - 'value' won't be evaluated.
        expr match {
            case Some(f) =>
                Some(() => {
                    val v = value()

                    (v, f.apply(v))
                })
            case _ =>
                throw new IgniteException("Invalid expression: " + exprStr)
        }
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
                var name: Option[String] = None
                var script: Option[String] = None
                val conditions = mutable.ArrayBuffer.empty[VisorAlertCondition]
                var freq = DFLT_FREQ
                var interval = DFLT_FREQ

                try {
                    args.foreach(arg => {
                        val (n, v) = arg

                        n match {
                            case c if alertDescr.contains(c) && v != null =>
                                val meta = alertDescr(c)

                                if (meta.byGrid)
                                    conditions += VisorAlertCondition(arg, gridFunc = makeGridFilter(v, meta.gridFunc))
                                else
                                    conditions += VisorAlertCondition(arg, nodeFunc = makeNodeFilter(v, meta.nodeFunc))

                            // Other tags.
                            case "n" if v != null => name = Some(v)
                            case "t" if v != null => freq = v.toLong
                            case "s" if v != null => script = Option(v)
                            case "i" if v != null => interval = v.toLong
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
                        scold(e)

                        break()
                }

                if (conditions.isEmpty) {
                    scold("No predicates have been provided in args: " + makeArgs(args))

                    break()
                }

                val alert = new VisorAlert(
                    id = id8,
                    name = name,
                    conditions = conditions.toSeq,
                    perGrid = conditions.exists(_.gridFunc.isDefined),
                    perNode = conditions.exists(_.nodeFunc.isDefined),
                    spec = makeArgs(args),
                    conditionSpec = makeArgs(conditions.map(_.arg)),
                    freq = freq,
                    createdOn = System.currentTimeMillis(),
                    varName = setVar(id8, "a"),
                    notification = new VisorAlertNotification(
                        script = script,
                        throttleInterval = interval * 1000
                    )
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

                    val node = ignite.cluster.node(discoEvt.eventNode().id())

                    if (node != null && !node.isDaemon)
                        alerts foreach (t => {
                            val (id, alert) = t

                            var check = (true, true)

                            val values = mutable.ArrayBuffer.empty[String]

                            try
                                check = alert.conditions.foldLeft(check) {
                                    (res, cond) => {
                                        val gridRes = cond.gridFunc.map(f => {
                                            val (value, check) = f()

                                            values += value.toString

                                            check
                                        }).getOrElse(true)

                                        val nodeRes = cond.nodeFunc.map(f => {
                                            val (value, check) = f(node)

                                            values += value.toString

                                            check
                                        }).getOrElse(true)

                                        (res._1 && gridRes) -> (res._2 && nodeRes)
                                    }
                                }
                            catch {
                                // In case of exception (like an empty projection) - simply return.
                                case _: Throwable => return true
                            }

                            if (check._1 && check._2) {
                                val now = System.currentTimeMillis()

                                var go: Boolean = false

                                go = (now - sent.getOrElse(id, 0L)) / 1000 >= alert.freq

                                if (go) {
                                    sent = sent + (id -> now)

                                    val stat: VisorStats = stats(id)

                                    assert(stat != null)

                                    // Update stats.
                                    if (stat.firstSnd == 0)
                                        stat.firstSnd = now

                                    stat.cnt += 1
                                    stat.lastSnd = now

                                    stats = stats + (id -> stat)

                                    // Write to Visor log if it is started (see 'log' command).
                                    logText(
                                        "Alert [" +
                                            "id=" + alert.id + "(@" + alert.varName + "), " +
                                            "spec=" + alert.spec + ", " +
                                            "created on=" + formatDateTime(alert.createdOn) +
                                        "]"
                                    )

                                    executeAlertScript(alert, node, values.toSeq)

                                    last10 = VisorSentAlert(
                                        id = alert.id,
                                        name = alert.name.getOrElse("(Not set)"),
                                        spec = alert.spec,
                                        createdOn = alert.createdOn,
                                        sentTs = now
                                    ) +: last10

                                    if (last10.size > 10)
                                        last10 = last10.take(10)
                                }
                            }
                            else
                                alert.notification.notified = false
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
            sum += ("First send", if (firstSnd == 0) NA else formatDateTime(firstSnd))
            sum += ("Last send", if (lastSnd == 0) NA else formatDateTime(lastSnd))

            sum.render()
        }

        if (last10.isEmpty)
            println("\nNo alerts have been sent.")
        else {
            val last10T = VisorTextTable()

            last10T #= ("ID(@)/Name", "Spec", "Sent", "Registered", "Count")

            last10.foreach((a: VisorSentAlert) => last10T += (
                a.idVar + "/" + a.name,
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

            tbl #= ("ID(@)/Name", "Spec", "Count", "Registered", "First Send", "Last Send")

            val sorted = alerts.values.toSeq.sortWith(_.varName < _.varName)

            sorted foreach (a => {
                val stat = stats(a.id)

                tbl += (
                    a.id + "(@" + a.varName + ')' + "/" + a.name.getOrElse("Not set"),
                    a.spec,
                    stat.cnt,
                    formatDateTime(a.createdOn),
                    if (stat.firstSnd == 0) NA else formatDateTime(stat.firstSnd),
                    if (stat.lastSnd == 0) NA else formatDateTime(stat.lastSnd)
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

    /**
     * Try to execute specified script on alert.
     *
     * @param alert Alarmed alert.
     * @param node Node where alert is alarmed.
     * @param values Values ith that alert is generated.
     */
    private def executeAlertScript(alert: VisorAlert, node: ClusterNode, values: Seq[String]) {
        val n = alert.notification

        try {
            n.script.foreach(script => {
                if (!n.notified && (n.notifiedTime < 0 || (System.currentTimeMillis() - n.notifiedTime) > n.throttleInterval)) {
                    val scriptFile = new File(script)

                    if (!scriptFile.exists())
                        throw new FileNotFoundException("Script/executable not found: " + script)

                    val scriptFolder = scriptFile.getParentFile

                    val p = Process(Seq(script, alert.name.getOrElse(alert.id), alert.conditionSpec) ++ values,
                        Some(scriptFolder))

                    p.run(ProcessLogger((fn: String) => {}))

                    n.notifiedTime = System.currentTimeMillis()

                    n.notified = true
                }
            })
        }
        catch {
            case e: Throwable => logText("Script execution failed [" +
                    "id=" + alert.id + "(@" + alert.varName + "), " +
                    "error=" + e.getMessage + ", " +
                "]")
        }
    }
}

/**
 * Visor alert.
 *
 * @param id Alert id.
 * @param name Alert name.
 * @param conditions List of alert condition metadata.
 * @param perGrid Condition per grid exists.
 * @param perNode Condition per node exists.
 * @param freq Freq of alert check.
 * @param spec Alert command specification.
 * @param conditionSpec Alert condition command specification.
 * @param varName Alert id short name.
 * @param createdOn Timestamp of alert creation.
 * @param notification Alert notification information.
 */
private case class VisorAlert(
    id: String,
    name: Option[String],
    conditions: Seq[VisorAlertCondition],
    perGrid: Boolean,
    perNode: Boolean,
    freq: Long,
    spec: String,
    conditionSpec: String,
    varName: String,
    createdOn: Long,
    notification: VisorAlertNotification
) {
    assert(id != null)
    assert(spec != null)
    assert(varName != null)
}

/**
  * Visor alert notification information.
  *
  * @param script Script to execute on alert firing.
  * @param throttleInterval Minimal interval between script execution.
  * @param notified `True` when on previous check of condition alert was fired or `false` otherwise.
  * @param notifiedTime Time of first alert notification in series of firings.
  */
private case class VisorAlertNotification(
    script: Option[String],
    throttleInterval: Long,
    var notified: Boolean = false,
    var notifiedTime: Long = -1L
) {
    assert(script != null)
}

/**
 * Visor alert condition information.
 *
 * @param arg Configuration argument.
 * @param gridFunc Function to check grid condition.
 * @param nodeFunc Function to check node condition.
 */
private case class VisorAlertCondition(
    arg: (String, String),
    gridFunc: Option[() => (Long, Boolean)] = None,
    nodeFunc: Option[(ClusterNode) => (Long, Boolean)] = None
)

/**
 * Snapshot of the sent alert.
 */
private case class VisorSentAlert(
    id: String,
    name: String,
    sentTs: Long,
    createdOn: Long,
    spec: String
) {
    assert(id != null)
    assert(spec != null)

    def idVar: String = {
        val v = mfindHead(id)

        if (v.isDefined) id + "(@" + v.get._1 + ")" else id
    }
}

/**
 * Statistics holder for visor alert.
 */
private case class VisorStats(
    var cnt: Int = 0,
    var firstSnd: Long = 0,
    var lastSnd: Long = 0
)

/**
  * Metadata object for visor alert.
  *
  * @param byGrid If `true` then `gridFunc` should be used.
  * @param gridFunc Function to extract value to check in case of alert for grid metrics.
  * @param nodeFunc Function to extract value to check in case of alert for node metrics
  */
private case class VisorAlertMeta(
    byGrid: Boolean,
    gridFunc: () => Long,
    nodeFunc: (ClusterNode) => Long
)

/**
 * Companion object that does initialization of the command.
 */
object VisorAlertCommand {
    /** Default alert frequency. */
    val DFLT_FREQ = 60L

    private val dfltNodeValF = (_: ClusterNode) => 0L
    private val dfltGridValF = () => 0L

    private def cl(): IgniteCluster = ignite.cluster()

    private[this] val BY_GRID = true
    private[this] val BY_NODE = false

    private val alertDescr = Map(
        "cc" -> VisorAlertMeta(BY_GRID, () => cl().metrics().getTotalCpus, dfltNodeValF),
        "nc" -> VisorAlertMeta(BY_GRID, () => cl().nodes().size, dfltNodeValF),
        "hc" -> VisorAlertMeta(BY_GRID, () => U.neighborhood(cl().nodes()).size, dfltNodeValF),
        "cl" -> VisorAlertMeta(BY_GRID, () => (cl().metrics().getAverageCpuLoad * 100).toLong, dfltNodeValF),
        "aj" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentActiveJobs),
        "cj" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentCancelledJobs),
        "tc" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentThreadCount),
        "ut" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getUpTime),
        "je" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentJobExecuteTime),
        "jw" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentJobWaitTime),
        "wj" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentWaitingJobs),
        "rj" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getCurrentRejectedJobs),
        "hu" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getHeapMemoryUsed),
        "cd" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => node.metrics().getHeapMemoryMaximum),
        "hm" -> VisorAlertMeta(BY_NODE, dfltGridValF, (node) => (node.metrics().getCurrentCpuLoad * 100).toLong))

    /** Singleton command. */
    private val cmd = new VisorAlertCommand

    addHelp(
        name = "alert",
        shortInfo = "Alerts for user-defined events.",
        longInfo = Seq(
            "Generates alerts for user-defined events.",
            "Node events and grid-wide events are defined via mnemonics."
        ),
        spec = Seq(
            "alert",
            "alert -u {-id=<alert-id>|-a}",
            "alert -r {-t=<sec>} {-<metric>=<condition><value>} ... {-<metric>=<condition><value>}"
        ),
        args = Seq(
            "-n" ->
                "Alert name",
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
                "If neither '-u' or '-r' provided - all alerts will be printed."
            ),
            "-t" -> Seq(
                "Defines notification frequency in seconds. Default is 60 seconds.",
                "This parameter can only appear with '-r'."
            ),
            "-s" -> Seq(
                "Define script for execution when alert triggered.",
                "For configuration of throttle period see -i argument.",
                "Script will receive following arguments:",
                "    1) Alert name or alert ID when name is not defined.",
                "    2) Alert condition as string.",
                "    3, ...) Values of alert conditions ordered as in alert command."
            ),
            "-i" -> "Configure alert notification minimal throttling interval in seconds. Default is 60 seconds.",
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
                "Notify every 15 min if grid has >= 4 CPUs and > 50% CPU load.",
            "alert \"-r -n=Nodes -t=15 -nc=gte3 -s=/home/user/scripts/alert.sh -i=300" ->
                ("Notify every 15 second if grid has >= 3 nodes and execute script \"/home/user/scripts/alert.sh\" with " +
                "repeat interval not less than 5 min.")
        ),

        emptyArgs = cmd.alert,
        withArgs = cmd.alert
    )

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
