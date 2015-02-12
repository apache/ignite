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

package org.apache.ignite.visor

import java.io._
import java.net._
import java.text._
import java.util.concurrent._
import java.util.{HashSet => JHashSet, _}

import org.apache.ignite.IgniteSystemProperties._
import org.apache.ignite.cluster.{ClusterGroup, ClusterMetrics, ClusterNode}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.events.EventType._
import org.apache.ignite.events.{DiscoveryEvent, Event}
import org.apache.ignite.internal.IgniteComponentType._
import org.apache.ignite.internal.IgniteNodeAttributes._
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException
import org.apache.ignite.internal.processors.spring.IgniteSpringProcessor
import org.apache.ignite.internal.{IgniteVersionUtils, IgniteEx}
import IgniteVersionUtils._
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.typedef._
import org.apache.ignite.internal.util.{GridConfigurationFinder, IgniteUtils}
import org.apache.ignite.internal.visor.VisorTaskArgument
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask
import org.apache.ignite.internal.visor.node.VisorNodeEventsCollectorTask.VisorNodeEventsCollectorTaskArg
import org.apache.ignite.lang.{IgniteNotPeerDeployable, IgnitePredicate}
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import org.apache.ignite.thread.IgniteThreadPoolExecutor
import org.apache.ignite.visor.commands.{VisorConsoleCommand, VisorTextTable}
import org.apache.ignite._
import org.jetbrains.annotations.Nullable

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.io.StdIn
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * Holder for command help information.
 */
sealed case class VisorConsoleCommandHolder(
    name: String,
    shortInfo: String,
    longInfo: Seq[String],
    aliases: Seq[String],
    spec: Seq[String],
    args: Seq[(String, AnyRef)],
    examples: Seq[(String, AnyRef)],
    impl: VisorConsoleCommand
    ) {
    /** Command host with optional aliases. */
    lazy val nameWithAliases: String =
        if (aliases != null && aliases.nonEmpty)
            name + " (" + ("" /: aliases)((b, a) => if (b.length() == 0) a else b + ", " + a) + ")"
        else
            name
}

/**
 * ==Overview==
 * This is the '''tagging''' trait existing solely to have type associated with
 * with `visor` object so that implicit conversions can be done
 * on `visor` object itself. Implicit conversions are essential to extensibility
 * of the Visor.
 *
 * ==Example==
 * This is an example on how [[VisorTag]] trait is used to
 * extend `visor` natively with custom commands:
 *
 * <ex>
 * class VisorCustomCommand {
 *     def foo(@Nullable args: String) = {
 *         if (visor.hasValue("bar", visor.parse(args)))
 *             println("foobar")
 *         else
 *             println("foo")
 *     }
 *     def foo(@Nullable args: Symbol*) = foo(visor.flatSymbols(args: _*))
 * }
 * object VisorCustomCommand {
 *     implicit def fromVisor(vs: VisorTag) = new VisorCustomCommand
 * }
 * </ex>
 */
trait VisorTag

/**
 * {{{
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 * }}}
 *
 * ==Overview==
 * Visor console provides monitoring capabilities for Ignite.
 *
 * ==Usage==
 * Ignite ships with `IGNITE_HOME/bin/ignitevisorcmd.{sh|bat}` script that starts Visor console.
 *
 * Just type:<ex>help</ex> in Visor console to get help and get started.
 */
@IgniteNotPeerDeployable
object visor extends VisorTag {
    /** Argument type. */
    type Arg = (String, String)

    /** Type alias for command argument list. */
    type ArgList = Seq[Arg]

    /** Type alias for general node filter. */
    type NodeFilter = ClusterNode => Boolean

    /** Type alias for general event filter. */
    type EventFilter = Event => Boolean

    /** `Nil` is for empty list, `Til` is for empty tuple. */
    val Til: Arg = (null, null)

    /** Node filter that includes any node. */
    final val ALL_NODES_FILTER = (_: ClusterNode) => true

    /** System line separator. */
    final val NL = System getProperty "line.separator"

    /** */
    private var cmdLst: Seq[VisorConsoleCommandHolder] = Nil

    /** Node left listener. */
    private var nodeLeftLsnr: IgnitePredicate[Event] = null

    /** Node join listener. */
    private var nodeJoinLsnr: IgnitePredicate[Event] = null

    /** Node segmentation listener. */
    private var nodeSegLsnr: IgnitePredicate[Event] = null

    /** Node stop listener. */
    private var nodeStopLsnr: IgnitionListener = null

    /** */
    @volatile private var isCon: Boolean = false

    /**
     * Whether or not Visor is the owner of connection - or it
     * reused one already opened.
     */
    @volatile private var conOwner: Boolean = false

    /** */
    @volatile private var conTs: Long = 0

    private final val LOC = Locale.US

    /** Date time format. */
    private final val dtFmt = new SimpleDateFormat("MM/dd/yy, HH:mm:ss", LOC)

    /** Date format. */
    private final val dFmt = new SimpleDateFormat("dd MMMM yyyy", LOC)

    private final val DEC_FMT_SYMS = new DecimalFormatSymbols(LOC)

    /** Number format. */
    private final val nmFmt = new DecimalFormat("#", DEC_FMT_SYMS)

    /** KB format. */
    private final val kbFmt = new DecimalFormat("###,###,###,###,###", DEC_FMT_SYMS)

    /** */
    private val mem = new ConcurrentHashMap[String, String]()

    /** List of close callbacks*/
    @volatile private var cbs = Seq.empty[() => Unit]

    /** List of shutdown callbacks*/
    @volatile private var shutdownCbs = Seq.empty[() => Unit]

    /** Default log file path. */
    /**
     * Default log file path. Note that this path is relative to `IGNITE_HOME/work` folder
     * if `IGNITE_HOME` system or environment variable specified, otherwise it is relative to
     * `work` folder under system `java.io.tmpdir` folder.
     */
    private final val DFLT_LOG_PATH = "visor/visor-log"

    /** Default configuration path relative to Ignite home. */
    private final val DFLT_CFG = "config/default-config.xml"

    /** Log file. */
    private var logFile: File = null

    /** Log timer. */
    private var logTimer: Timer = null

    /** Topology log timer. */
    private var topTimer: Timer = null

    /** Log started flag. */
    @volatile private var logStarted = false

    /** Internal thread pool. */
    @volatile var pool: ExecutorService = new IgniteThreadPoolExecutor()

    /** Configuration file path, if any. */
    @volatile var cfgPath: String = null

    /** */
    @volatile var ignite: IgniteEx = null

    /**
     * Get grid node for specified ID.
     *
     * @param nid Node ID.
     * @return GridNode instance.
     * @throws IgniteException if Visor is disconnected or node not found.
     */
    def node(nid: UUID): ClusterNode = {
        val g = ignite

        if (g == null)
            throw new IgniteException("Visor disconnected")
        else {
            val node = g.node(nid)

            if (node == null)
                throw new IgniteException("Node is gone: " + nid)

            node
        }
    }

    Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
            try
                if (ignite != null && isConnected) {
                    // Call all shutdown callbacks.
                    shutdownCbs foreach(_.apply())

                    close() // This will stop the grid too if Visor is connection owner.
                }
            catch {
                case ignore: Throwable => // ignore
            }
        }
    })

    addHelp(
        name = "mlist",
        shortInfo = "Prints Visor console memory variables.",
        spec = Seq(
            "mlist {arg}"
        ),
        args = Seq(
            "arg" ->
                "String that contains start characters of variable names."
        ),
        examples = Seq(
            "mlist" ->
                "Prints out all Visor console memory variables.",
            "mlist ac" ->
                "Lists variables that start with 'a' or 'c' from Visor console memory."
        ),
        ref = VisorConsoleCommand(mlist, mlist)
    )

    addHelp(
        name = "mclear",
        shortInfo = "Clears Visor console memory variables.",
        spec = Seq(
            "mclear",
            "mclear <name>|-ev|-al|-ca|-no|-tn|-ex"
        ),
        args = Seq(
            "<name>" -> Seq(
                "Variable name to clear.",
                "Note that name doesn't include '@' symbol used to reference variable."
            ),
            "-ev" ->
                "Clears all 'event' variables.",
            "-al" ->
                "Clears all 'alert' variables.",
            "-ca" ->
                "Clears all 'cache' variables.",
            "-no" ->
                "Clears all 'node' variables.",
            "-tn" ->
                "Clears all 'task name' variables.",
            "-ex" ->
                "Clears all 'task execution' variables."
        ),
        examples = Seq(
            "mclear" ->
                "Clears all Visor console variables.",
            "mclear -ca" ->
                "Clears all Visor console cache variables.",
            "mclear n2" ->
                "Clears 'n2' Visor console variable."
        ),
        ref = VisorConsoleCommand(mclear, mclear)
    )

    addHelp(
        name = "mget",
        shortInfo = "Gets Visor console memory variable.",
        longInfo = Seq(
            "Gets Visor console memory variable. Variable can be referenced with '@' prefix."
        ),
        spec = Seq(
            "mget <@v>"
        ),
        args = Seq(
            "@v" ->
                "Variable name."
        ),
        examples = Seq(
            "mget <@v>" ->
                "Gets Visor console variable whose name is referenced by variable 'v'."
        ),
        ref = VisorConsoleCommand(mget, mget)
    )

    addHelp(
        name = "help",
        shortInfo = "Prints Visor console help.",
        aliases = Seq("?"),
        spec = Seq(
            "help {c1 c2 ... ck}"
        ),
        args = Seq(
            "ck" ->
                "Command to get help for."
        ),
        examples = Seq(
            "help status" ->
                "Prints help for 'status' command.",
            "help" ->
                "Prints help for all command."
        ),
        ref = VisorConsoleCommand(help, help)
    )

    addHelp(
        name = "status",
        shortInfo = "Prints Visor console status.",
        aliases = Seq("!"),
        spec = Seq(
            "status {-q}"
        ),
        args = Seq(
            "-q" ->
                "Quite output without ASCII logo."
        ),
        examples = Seq(
            "status" ->
                "Prints Visor console status.",
            "status -q" ->
                "Prints Visor console status in quiet mode."
        ),
        ref = VisorConsoleCommand(status, status)
    )

    addHelp(
        name = "open",
        shortInfo = "Connects Visor console to the grid.",
        longInfo = Seq(
            "Connects Visor console to the grid. Note that P2P class loading",
            "should be enabled on all nodes.",
            " ",
            "If neither '-cpath' or '-d' are provided, command will ask",
            "user to select Ignite configuration file in interactive mode."
        ),
        spec = Seq(
            "open -cpath=<path>",
            "open -d"
        ),
        args = Seq(
            "-cpath=<path>" -> Seq(
                "Ignite configuration path.",
                "Can be absolute, relative to Ignite home folder or any well formed URL."
            ),
            "-d" -> Seq(
                "Flag forces the command to connect to grid using default Ignite configuration file.",
                "without interactive mode."
            )
        ),
        examples = Seq(
            "open" ->
                "Prompts user to select Ignite configuration file in interactive mode.",
            "open -d" ->
                "Connects Visor console to grid using default Ignite configuration file.",
            "open -cpath=/gg/config/mycfg.xml" ->
                "Connects Visor console to grid using Ignite configuration from provided file."
        ),
        ref = VisorConsoleCommand(open, open)
    )

    addHelp(
        name = "close",
        shortInfo = "Disconnects Visor console from the grid.",
        spec = Seq("close"),
        examples = Seq(
            "close" ->
                "Disconnects Visor console from the grid."
        ),
        ref = VisorConsoleCommand(close)
    )

    addHelp(
        name = "quit",
        shortInfo = "Quit from Visor console.",
        spec = Seq("quit"),
        examples = Seq(
            "quit" ->
                "Quit from Visor console."
        ),
        aliases = Seq("exit"),
        ref = VisorConsoleCommand(quit)
    )

    addHelp(
        name = "log",
        shortInfo = "Starts or stops grid-wide events logging.",
        longInfo = Seq(
            "Logging of discovery and failure grid-wide events.",
            "Logging starts by default when Visor starts.",
            " ",
            "Events are logged to a file. If path is not provided,",
            "it will log into '<Ignite home folder>/work/visor/visor-log'.",
            " ",
            "File is always opened in append mode.",
            "If file doesn't exist, it will be created.",
            " ",
            "It is often convenient to 'tail -f' the log file",
            "in a separate console window.",
            " ",
            "Log command prints periodic topology snapshots in the following format:",
            "H/N/C |1   |1   |4   |=^========..........|",
            "where:",
            "   H - Hosts",
            "   N - Nodes",
            "   C - CPUs",
            "   = - 5%-based marker of average CPU load across the topology",
            "   ^ - 5%-based marker of average heap memory used across the topology"
        ),
        spec = Seq(
            "log",
            "log -l {-f=<path>} {-p=<num>} {-t=<num>} {-dl}",
            "log -s"
        ),
        args = Seq(
            "-l" -> Seq(
                "Starts logging.",
                "If logging is already started - it's no-op."
            ),
            "-f=<path>" -> Seq(
                "Provides path to the file.",
                "Path to the file can be absolute or relative to Ignite home folder."
            ),
            "-p=<num>" -> Seq(
                "Provides period of querying events (in seconds).",
                "Default is 10."
            ),
            "-t=<num>" -> Seq(
                "Provides period of logging topology snapshot (in seconds).",
                "Default is 20."
            ),
            "-s" -> Seq(
                "Stops logging.",
                "If logging is already stopped - it's no-op."
            ),
            "-dl" -> Seq(
                "Disables collecting of job and task fail events, licence violation events, cache preloading events" +
                    " from remote nodes."
            )
        ),
        examples = Seq(
            "log" ->
                "Prints log status.",
            "log -l -f=/home/user/visor-log" ->
                "Starts logging to file 'visor-log' located at '/home/user'.",
            "log -l -f=log/visor-log" ->
                "Starts logging to file 'visor-log' located at '<Ignite home folder>/log'.",
            ("log -l -p=20",
                "Starts logging to file '<Ignite home folder>/work/visor/visor-log' " +
                "with querying events period of 20 seconds."),
            ("log -l -t=30",
                "Starts logging to file '<Ignite home folder>/work/visor/visor-log' " +
                "with topology snapshot logging period of 30 seconds."),
            ("log -l -dl",
                "Starts logging to file '<Ignite home folder>/work/visor/visor-log' " +
                "with disabled collection events from remote nodes."),
            "log -s" ->
                "Stops logging."
        ),
        ref = VisorConsoleCommand(log, log)
    )

    logText("Visor started.")

    // Print out log explanation at the beginning.
    logText("<log>: H - Hosts")
    logText("<log>: N - Nodes")
    logText("<log>: C - CPUs")
    logText("<log>: = - 5%-based marker of average CPU load across the topology")
    logText("<log>: ^ - 5%-based marker of average heap memory used across the topology")

    /**
     * ==Command==
     * Lists Visor console memory variables.
     *
     * ==Examples==
     * <ex>mlist ac</ex>
     * Lists variables that start with `a` or `c` from Visor console memory.
     *
     * <ex>mlist</ex>
     * Lists all variables from Visor console memory.
     *
     * @param arg String that contains start characters of listed variables.
     *      If empty - all variables will be listed.
     */
    def mlist(arg: String) {
        assert(arg != null)

        if (mem.isEmpty)
            println("Memory is empty.")
        else {
            val r = if (arg.trim == "") mem.toMap else mem.filter { case (k, _) => arg.contains(k.charAt(0)) }

            if (r.isEmpty)
                println("No matches found.")
            else {
                val t = new VisorTextTable()

                t.maxCellWidth = 70

                t #= ("Name", "Value")

                r.toSeq.sortBy(_._1).foreach { case (k, v) => t += (k, v) }

                t.render()

                nl()

                println(
                    "Variable can be referenced in other commands with '@' prefix." + NL +
                        "Reference can be either a flag or a parameter value." + NL +
                        "\nEXAMPLE: " + NL +
                        "    'help @cmd' - where 'cmd' variable contains command name." + NL +
                        "    'node -id8=@n11' - where 'n11' variable contains node ID8."
                )
            }
        }
    }

    /**
     * Shortcut for `println()`.
     */
    def nl() {
        println()
    }

    /**
     * ==Command==
     * Lists all Visor console memory.
     *
     * ==Examples==
     * <ex>mlist</ex>
     * Lists all variables in Visor console memory.
     */
    def mlist() {
        mlist("")
    }

    /**
     * Clears given Visor console variable or the whole namespace.
     *
     * @param arg Variable host or namespace mnemonic.
     */
    def mclear(arg: String) {
        assert(arg != null)

        arg match {
            case "-ev" => clearNamespace("e")
            case "-al" => clearNamespace("a")
            case "-ca" => clearNamespace("c")
            case "-no" => clearNamespace("n")
            case "-tn" => clearNamespace("t")
            case "-ex" => clearNamespace("s")
            case _ => mem.remove(arg)
        }
    }

    /**
     * Clears given variable namespace.
     *
     * @param namespace Namespace.
     */
    private def clearNamespace(namespace: String) {
        assert(namespace != null)

        mem.keySet.foreach(k => {
            if (k.startsWith(namespace))
                try {
                    k.substring(1).toInt

                    mem.remove(k)
                }
                catch {
                    case ignored: Throwable => // No-op.
                }
        })
    }

    /**
     * Clears all Visor console memory.
     */
    def mclear() {
        mem.clear()
    }

    /**
     * Finds variable by its value.
     *
     * @param v Value to find by.
     */
    def mfind(@Nullable v: String): Option[(String, String)] =
        mem find(t => t._2 == v)

    /**
     * Sets Visor console memory variable. Note that this method '''does not'''
     * perform variable substitution on its parameters.
     *
     * @param n Name of the variable. Can't be `null`.
     * @param v Value of the variable. Can't be `null`.
     * @return Previous value.
     */
    def mset(n: String, v: String): String = {
        msetOpt(n, v).orNull
    }

    /**
     * Sets Visor console memory variable. Note that this method '''does not'''
     * perform variable substitution on its parameters.
     *
     * @param n Name of the variable. Can't be `null`.
     * @param v Value of the variable. Can't be `null`.
     * @return Previous value as an option.
     */
    def msetOpt(n: String, v: String): Option[String] = {
        assert(n != null)
        assert(v != null)

        val prev = mem.get(n)

        mem.put(n, v)

        Option(prev)
    }

    /**
     * ==Command==
     * Gets Visor console memory variable. Note that this method '''does not'''
     * perform variable substitution on its parameters.
     *
     * ==Examples==
     * <ex>mget @a</ex>
     * Gets the value for Visor console variable '@a'.
     *
     * @param n Name of the variable.
     * @return Variable value or `null` if such variable doesn't exist or its value was set as `null`.
     */
    def mget(n: String) {
        val key = if (n.startsWith("@")) n.substring(1) else n

        if (mem.containsKey(key)) {
            val t = new VisorTextTable()

            t.maxCellWidth = 70

            t #= ("Name", "Value")

            t += (n, mem.get(key))

            t.render()

            nl()
        }
        else {
            warn("Missing variable with name: \'" + n + "\'.")
        }
    }

    /**
     * Trap for missing arguments.
     */
    def mget() {
        warn("Missing argument.")
        warn("Type 'help mget' to see how to use this command.")
    }

    /**
     * ==Command==
     * Gets Visor console memory variable. Note that this method '''does not'''
     * perform variable substitution on its parameters.
     *
     * ==Examples==
     * <ex>mgetOpt a</ex>
     * Gets the value as an option for Visor console variable 'a'.
     *
     * @param n Name of the variable.
     * @return Variable host as an option.
     */
    def mgetOpt(n: String): Option[String] = {
        assert(n != null)

        Option(mem.get(n))
    }

    /**
     * If variable with given value and prefix doesn't exist - creates
     * a new variable with given value and returns its host. Otherwise,
     * returns an existing variable host.
     *
     * @param v Value.
     * @param prefix Variable host prefix.
     * @return Existing variable host or the new variable host.
     */
    def setVarIfAbsent(v: AnyRef, prefix: String): String = {
        assert(v != null)
        assert(prefix != null && prefix.length > 0)

        val s = v.toString

        val t = mem.find((t: (String, String)) => t._1.startsWith(prefix) && t._2 == s)

        if (t.isDefined)
            t.get._1
        else {
            for (i <- 0 until Int.MaxValue if mem.putIfAbsent(prefix + i, s) == null)
                return prefix + i

            throw new IgniteException("No more memory.")
        }
    }

    /**
     * Try get variable value with given name.
     *
     * @param v variable name.
     * @return variable value or `v` if variable with name `v` not exist.
     */
    def getVariable(v: String): String = {
        v match {
            case name if name.startsWith("@") => mgetOpt(name.substring(1)).getOrElse(v)
            case _ => v
        }
    }

    /**
     * Creates a new variable with given value and returns its host.
     *
     * @param v Value.
     * @param prefix Variable host prefix.
     * @return New variable host.
     */
    def setVar(v: AnyRef, prefix: String): String = {
        assert(v != null)
        assert(prefix != null && prefix.length > 0)

        val s = v.toString

        for (i <- 0 until Int.MaxValue if mem.putIfAbsent(prefix + i, s) == null)
            return prefix + i

        throw new IgniteException("No more memory.")
    }

    /**
     * Adds command help to the Visor console. This will be printed as part of `help` command.
     *
     * @param name Command name.
     * @param shortInfo Short command description.
     * @param longInfo Optional multi-line long command description. If not provided - short description
     *      will be used instead.
     * @param aliases List of aliases. Optional.
     * @param spec Command specification.
     * @param args List of `(host, description)` tuples for command arguments. Optional.
     * @param examples List of `(example, description)` tuples for command examples.
     * @param ref - command implementation.
     */
    def addHelp(
        name: String,
        shortInfo: String,
        @Nullable longInfo: Seq[String] = null,
        @Nullable aliases: Seq[String] = Seq.empty,
        spec: Seq[String],
        @Nullable args: Seq[(String, AnyRef)] = null,
        examples: Seq[(String, AnyRef)],
        ref: VisorConsoleCommand) {
        assert(name != null)
        assert(shortInfo != null)
        assert(spec != null && spec.nonEmpty)
        assert(examples != null && examples.nonEmpty)
        assert(ref != null)

        // Add and re-sort
        cmdLst = (cmdLst ++ Seq(VisorConsoleCommandHolder(name, shortInfo, longInfo, aliases, spec, args, examples, ref))).
            sortWith((a, b) => a.name.compareTo(b.name) < 0)
    }

    /**
     * Extract node from command arguments.
     *
     * @param argLst Command arguments.
     * @return error message or node ref.
     */
    def parseNode(argLst: ArgList) = {
        val id8 = argValue("id8", argLst)
        val id = argValue("id", argLst)

        if (id8.isDefined && id.isDefined)
            Left("Only one of '-id8' or '-id' is allowed.")
        else if (id8.isDefined) {
            nodeById8(id8.get) match {
                case Nil => Left("Unknown 'id8' value: " + id8.get)
                case node :: Nil => Right(Option(node))
                case _ => Left("'id8' resolves to more than one node (use full 'id' instead): " + id8.get)
            }
        }
        else if (id.isDefined)
            try {
                val node = Option(ignite.node(java.util.UUID.fromString(id.get)))

                if (node.isDefined)
                    Right(node)
                else
                    Left("'id' does not match any node: " + id.get)
            }
            catch {
                case e: IllegalArgumentException => Left("Invalid node 'id': " + id.get)
            }
        else
            Right(None)
    }

    private[this] def parseArg(arg: String): Arg = {
        if (arg(0) == '-' || arg(0) == '/') {
            val eq = arg.indexOf('=')

            if (eq == -1)
                arg.substring(1) -> null
            else {
                val n = arg.substring(1, eq).trim
                var v = arg.substring(eq + 1).trim.replaceAll("['\"`]$", "").replaceAll("^['\"`]", "")

                if (v.startsWith("@"))
                    v = mgetOpt(v.substring(1)).getOrElse(v)

                n -> v
            }
        }
        else {
            val k: String = null

            val v = if (arg.startsWith("@"))
                mgetOpt(arg.substring(1)).getOrElse(arg)
            else
                arg

            k -> v
        }
    }

    private val quotedArg = "(?:[-/].*=)?(['\"`]).*".r

    /**
     * Utility method that parses command arguments. Arguments represented as a string
     * into argument list represented as list of tuples (host, value) performing
     * variable substitution:
     *
     * `-p=@n` - A named parameter where `@n` will be considered as a reference to variable named `n`.
     * `@ n` - An unnamed parameter where `@n` will be considered as a reference to variable named `n`.
     * `-p` - A flag doesn't support variable substitution.
     *
     * Note that recursive substitution isn't supported. If specified variable isn't set - the value
     * starting with `@` will be used as-is.
     *
     * @param args Command arguments to parse.
     */
    def parseArgs(@Nullable args: String): ArgList = {
        val buf = collection.mutable.ArrayBuffer.empty[Arg]

        if (args != null && args.trim.nonEmpty) {
            val lst = args.trim.split(" ")

            val sb = new StringBuilder()

            for (i <- 0 until lst.size if lst(i).nonEmpty || sb.size != 0) {
                val arg = sb.toString + lst(i)

                arg match {
                    case quotedArg(quote) if arg.count(_ == quote(0)) % 2 != 0 && i + 1 < lst.size =>
                        sb.append(lst(i)).append(" ")

                    case _ =>
                        sb.clear()

                        buf += parseArg(arg)
                }
            }
        }

        buf
    }

    /**
     * Shortcut method that checks if passed in argument list has an argument with given value.
     *
     * @param v Argument value to check for existence in this list.
     * @param args Command argument list.
     */
    def hasArgValue(@Nullable v: String, args: ArgList): Boolean = {
        assert(args != null)

        args.find(_._2 == v).nonEmpty
    }

    /**
     * Shortcut method that checks if passed in argument list has an argument with given host.
     *
     * @param n Argument host to check for existence in this list.
     * @param args Command argument list.
     */
    def hasArgName(@Nullable n: String, args: ArgList): Boolean = {
        assert(args != null)

        args.find(_._1 == n).nonEmpty
    }

    /**
     * Shortcut method that checks if flag (non-`null` host and `null` value) is set
     * in the argument list.
     *
     * @param n Name of the flag.
     * @param args Command argument list.
     */
    def hasArgFlag(n: String, args: ArgList): Boolean = {
        assert(n != null && args != null)

        args.find((a) => a._1 == n && a._2 == null).nonEmpty
    }

    /**
     * Gets the value for a given argument host.
     *
     * @param n Argument host.
     * @param args Argument list.
     * @return Argument value.
     */
    @Nullable def argValue(n: String, args: ArgList): Option[String] = {
        assert(n != null && args != null)

        Option((args find(_._1 == n) getOrElse Til)._2)
    }

    /**
     * Gets a non-`null` value for given parameter.
     *
     * @param a Parameter.
     * @param dflt Value to return if `a` is `null`.
     */
    def safe(@Nullable a: Any, dflt: Any = ""): String = {
        assert(dflt != null)

        if (a != null) a.toString else dflt.toString
    }

    /**
     * Reconstructs string presentation for given argument.
     *
     * @param arg Argument to reconstruct.
     */
    @Nullable def makeArg(arg: Arg): String = {
        assert(arg != null)

        var s = ""

        if (arg._1 != null) {
            s = "-" + arg._1

            if (arg._2 != null)
                s = s + '=' + arg._2
        }
        else
            s = arg._2

        s
    }

    /**
     * Reconstructs string presentation for given argument list.
     *
     * @param args Argument list to reconstruct.
     */
    def makeArgs(args: ArgList): String = {
        assert(args != null)

        ("" /: args)((b, a) => if (b.length == 0) makeArg(a) else b + ' ' + makeArg(a))
    }

    /**
     * Parses string containing mnemonic predicate and returns Scala predicate.
     *
     * @param s Mnemonic predicate.
     * @return Long to Boolean predicate or null if predicate cannot be created.
     */
    def makeExpression(s: String): Option[Long => Boolean] = {
        assert(s != null)

        def value(v: String): Long =
            // Support for seconds, minutes and hours.
            // NOTE: all memory sizes are assumed to be in MB.
            v.last match {
                case 's' => v.substring(0, v.length - 1).toLong * 1000
                case 'm' => v.substring(0, v.length - 1).toLong * 1000 * 60
                case 'h' => v.substring(0, v.length - 1).toLong * 1000 * 60 * 60
                case _ => v.toLong
            }

        try
            if (s.startsWith("lte")) // <=
                Some(_ <= value(s.substring(3)))
            else if (s.startsWith("lt")) // <
                Some(_ < value(s.substring(2)))
            else if (s.startsWith("gte")) // >=
                Some(_ >= value(s.substring(3)))
            else if (s.startsWith("gt")) // >
                Some(_ > value(s.substring(2)))
            else if (s.startsWith("eq")) // ==
                Some(_ == value(s.substring(2)))
            else if (s.startsWith("neq")) // !=
                Some(_ != value(s.substring(3)))
            else
                None
        catch {
            case e: Throwable => None
        }
    }

    // Formatters.
    private val dblFmt = new DecimalFormat("#0.00", DEC_FMT_SYMS)
    private val intFmt = new DecimalFormat("#0", DEC_FMT_SYMS)

    /**
     * Formats double value with `#0.00` formatter.
     *
     * @param d Double value to format.
     */
    def formatDouble(d: Double): String = {
        dblFmt.format(d)
    }

    /**
     * Formats double value with `#0` formatter.
     *
     * @param d Double value to format.
     */
    def formatInt(d: Double): String = {
        intFmt.format(d.round)
    }

    /**
     * Returns string representation of the timestamp provided. Result formatted
     * using pattern `MM/dd/yy, HH:mm:ss`.
     *
     * @param ts Timestamp.
     */
    def formatDateTime(ts: Long): String =
        dtFmt.format(ts)

    /**
     * Returns string representation of the date provided. Result formatted using
     * pattern `MM/dd/yy, HH:mm:ss`.
     *
     * @param date Date.
     */
    def formatDateTime(date: Date): String =
        dtFmt.format(date)

    /**
     * Returns string representation of the timestamp provided. Result formatted
     * using pattern `MM/dd/yy`.
     *
     * @param ts Timestamp.
     */
    def formatDate(ts: Long): String =
        dFmt.format(ts)

    /**
     * Returns string representation of the date provided. Result formatted using
     * pattern `MM/dd/yy`.
     *
     * @param date Date.
     */
    def formatDate(date: Date): String =
        dFmt.format(date)

    /**
     * Base class for memory units.
     *
     * @param name Unit name to display on screen.
     * @param base Unit base to convert from bytes.
     */
    private[this] sealed abstract class VisorMemoryUnit(name: String, val base: Long) {
        /**
         * Convert memory in bytes to memory in units.
         *
         * @param m Memory in bytes.
         * @return Memory in units.
         */
        def toUnits(m: Long): Double = m.toDouble / base

        /**
         * Check if memory fits measure units.
         *
         * @param m Memory in bytes.
         * @return `True` if memory is more than `1` after converting bytes to units.
         */
        def has(m: Long): Boolean = toUnits(m) >= 1

        override def toString = name
    }

    private[this] case object BYTES extends VisorMemoryUnit("b", 1)
    private[this] case object KILOBYTES extends VisorMemoryUnit("kb", 1024L)
    private[this] case object MEGABYTES extends VisorMemoryUnit("mb", 1024L * 1024L)
    private[this] case object GIGABYTES extends VisorMemoryUnit("gb", 1024L * 1024L * 1024L)
    private[this] case object TERABYTES extends VisorMemoryUnit("tb", 1024L * 1024L * 1024L * 1024L)

    /**
     * Detect memory measure units: from BYTES to TERABYTES.
     *
     * @param m Memory in bytes.
     * @return Memory measure units.
     */
    private[this] def memoryUnit(m: Long): VisorMemoryUnit =
        if (TERABYTES.has(m))
            TERABYTES
        else if (GIGABYTES.has(m))
            GIGABYTES
        else if (MEGABYTES.has(m))
            MEGABYTES
        else if (KILOBYTES.has(m))
            KILOBYTES
        else
            BYTES

    /**
     * Returns string representation of the memory.
     *
     * @param n Memory size.
     */
    def formatMemory(n: Long): String = {
        if (n > 0) {
            val u = memoryUnit(n)

            kbFmt.format(u.toUnits(n)) + u.toString
        }
        else
            "0"
    }

    /**
     * Returns string representation of the memory limit.
     *
     * @param n Memory size.
     */
    def formatMemoryLimit(n: Long): String = {
        n match {
            case -1 => "Disabled"
            case 0 => "Unlimited"
            case m => formatMemory(m)
        }
    }

    /**
     * Returns string representation of the number.
     *
     * @param n Number.
     */
    def formatNumber(n: Long): String =
        nmFmt.format(n)

    /**
     * Tests whether or not Visor console is connected.
     *
     * @return `True` if Visor console is connected.
     */
    def isConnected =
        isCon

    /**
     * Gets timestamp of Visor console connection. Returns `0` if Visor console is not connected.
     *
     * @return Timestamp of Visor console connection.
     */
    def connectTimestamp =
        conTs

    /**
     * Prints properly formatted error message like:
     * {{{
     * <visor>: err: error message
     * }}}
     *
     * @param errMsgs Error messages to print. If `null` - this function is no-op.
     */
    def warn(errMsgs: Any*) {
        assert(errMsgs != null)

        errMsgs foreach (msg => scala.Console.out.println("(wrn) <visor>: " + msg))
    }

    /**
     * Prints standard 'not connected' error message.
     */
    def adviseToConnect() {
        warn(
            "Visor is disconnected.",
            "Type 'open' to connect Visor console or 'help open' to get help."
        )
    }

    /**
     * Gets global projection as an option.
     */
    def gridOpt =
        Option(ignite)

    def noop() {}

    /**
     * ==Command==
     * Prints Visor console status.
     *
     * ==Example==
     * <ex>status -q</ex>
     * Prints Visor console status without ASCII logo.
     *
     * @param args Optional "-q" flag to disable ASCII logo printout.
     */
    def status(args: String) {
        val argLst = parseArgs(args)

        if (!hasArgFlag("q", argLst))
            println(
                " ___    _________________________ ________" + NL +
                " __ |  / /____  _/__  ___/__  __ \\___  __ \\" + NL +
                " __ | / /  __  /  _____ \\ _  / / /__  /_/ /" + NL +
                " __ |/ /  __/ /   ____/ / / /_/ / _  _, _/" + NL +
                " _____/   /___/   /____/  \\____/  /_/ |_|" + NL + NL +
                " ADMIN CONSOLE" + NL +
                " " + COPYRIGHT + NL
            )

        val t = VisorTextTable()

        t += ("Status", if (isCon) "Connected" else "Disconnected")
        t += ("Grid name",
            if (ignite == null)
                "<n/a>"
            else {
                val n = ignite.name

                if (n == null) "<default>" else n
            }
        )
        t += ("Config path", safe(cfgPath, "<n/a>"))
        t += ("Uptime", if (isCon) X.timeSpan2HMS(uptime) else "<n/a>")

        t.render()
    }

    /**
     * ==Command==
     * Prints Visor console status (with ASCII logo).
     *
     * ==Example==
     * <ex>status</ex>
     * Prints Visor console status.
     */
    def status() {
        status("")
    }

    /**
     * ==Command==
     * Prints help for specific command(s) or for all commands.
     *
     * ==Example==
     * <ex>help</ex>
     * Prints general help.
     *
     * <ex>help open</ex>
     * Prints help for 'open' command.
     *
     * @param args List of commands to print help for. If empty - prints generic help.
     */
    def help(args: String = null) {
        val argLst = parseArgs(args)

        if (!has(argLst)) {
            val t = VisorTextTable()

            t.autoBorder = false

            t.maxCellWidth = 55

            t #= ("Command", "Description")

            cmdLst foreach (hlp => t += (hlp.nameWithAliases, hlp.shortInfo))

            t.render()

            println("\nType 'help \"command name\"' to see how to use this command.")
        }
        else
            for (c <- argLst)
                if (c._1 != null)
                    warn("Invalid command name: " + argName(c))
                else if (c._2 == null)
                    warn("Invalid command name: " + argName(c))
                else {
                    val n = c._2

                    val opt = cmdLst.find(_.name == n)

                    if (opt.isEmpty)
                        warn("Invalid command name: " + n)
                    else {
                        val hlp: VisorConsoleCommandHolder = opt.get

                        val t = VisorTextTable()

                        t += (hlp.nameWithAliases, if (hlp.longInfo == null) hlp.shortInfo else hlp.longInfo)

                        t.render()

                        println("\nSPECIFICATION:")

                        hlp.spec foreach(s => println(blank(4) + s))

                        if (has(hlp.args)) {
                            println("\nARGUMENTS:")

                            hlp.args foreach (a => {
                                val (arg, desc) = a

                                println(blank(4) + arg)

                                desc match {
                                    case (lines: Iterable[_]) => lines foreach (line => println(blank(8) + line))
                                    case s: AnyRef => println(blank(8) + s.toString)
                                }
                            })
                        }

                        if (has(hlp.examples)) {
                            println("\nEXAMPLES:")

                            hlp.examples foreach (a => {
                                val (ex, desc) = a

                                println(blank(4) + ex)

                                desc match {
                                    case (lines: Iterable[_]) => lines foreach (line => println(blank(8) + line))
                                    case s: AnyRef => println(blank(8) + s.toString)
                                }
                            })
                        }

                        nl()
                    }
                }
    }

    /**
     * Tests whether passed in sequence is not `null` and not empty.
     */
    private def has[T](@Nullable s: Seq[T]): Boolean = {
        s != null && s.nonEmpty
    }

    /**
     * ==Command==
     * Prints generic help.
     *
     * ==Example==
     * <ex>help</ex>
     * Prints help.
     */
    def help() {
        help("")
    }

    /**
     * Helper function that makes up the full argument host from tuple.
     *
     * @param t Command argument tuple.
     */
    def argName(t: (String, String)): String =
        if (F.isEmpty(t._1) && F.isEmpty(t._2))
            "<empty>"
        else if (F.isEmpty(t._1))
            t._2
        else
            t._1

    /**
     * Helper method that produces blank string of given length.
     *
     * @param len Length of the blank string.
     */
    private def blank(len: Int) = new String().padTo(len, ' ')

    /**
     * ==Command==
     * Connects Visor console to default or named grid.
     *
     * ==Examples==
     * <ex>open -g=mygrid</ex>
     * Connects to 'mygrid' grid.
     *
     * @param args Command arguments.
     */
    def open(args: String) {
        assert(args != null)

        if (isConnected) {
            warn("Visor is already connected. Disconnect first.")

            return
        }

        try {
            def configuration(path: String): IgniteConfiguration = {
                assert(path != null)

                val url =
                    try
                        new URL(path)
                    catch {
                        case e: Exception =>
                            val url = IgniteUtils.resolveIgniteUrl(path)

                            if (url == null)
                                throw new IgniteException("Ignite configuration path is invalid: " + path, e)

                            url
                    }

                // Add no-op logger to remove no-appender warning.
                val log4jTup =
                    if (classOf[Ignition].getClassLoader.getResource("org/apache/log4j/Appender.class") != null)
                        IgniteUtils.addLog4jNoOpLogger()
                    else
                        null

                val spring: IgniteSpringProcessor = SPRING.create(false)

                val cfgs =
                    try
                        // Cache, GGFS, streamer and DR configurations should be excluded from daemon node config.
                        spring.loadConfigurations(url, "cacheConfiguration", "ggfsConfiguration", "streamerConfiguration",
                            "drSenderHubConfiguration", "drReceiverHubConfiguration").get1()
                    finally {
                        if (log4jTup != null)
                            IgniteUtils.removeLog4jNoOpLogger(log4jTup)
                    }

                if (cfgs == null || cfgs.isEmpty)
                    throw new IgniteException("Can't find grid configuration in: " + url)

                if (cfgs.size > 1)
                    throw new IgniteException("More than one grid configuration found in: " + url)

                val cfg = cfgs.iterator().next()

                // Setting up 'Config URL' for properly print in console.
                System.setProperty(IgniteSystemProperties.IGNITE_CONFIG_URL, url.getPath)

                var cpuCnt = Runtime.getRuntime.availableProcessors

                if (cpuCnt < 4)
                    cpuCnt = 4

                cfg.setConnectorConfiguration(null)

                // All thread pools are overridden to have size equal to number of CPUs.
                cfg.setPublicThreadPoolSize(cpuCnt)
                cfg.setSystemThreadPoolSize(cpuCnt)
                cfg.setPeerClassLoadingThreadPoolSize(cpuCnt)

                var ioSpi = cfg.getCommunicationSpi

                if (ioSpi == null)
                    ioSpi = new TcpCommunicationSpi()

                cfg
            }

            val argLst = parseArgs(args)

            val path = argValue("cpath", argLst)
            val dflt = hasArgFlag("d", argLst)

            val (cfg, cfgPath) =
                if (path.isDefined)
                    (configuration(path.get), path.get)
                else if (dflt)
                    (configuration(DFLT_CFG), "<default>")
                else {
                    // If configuration file is not defined in arguments,
                    // ask to choose from the list
                    askConfigFile() match {
                        case Some(p) =>
                            nl()

                            (VisorTextTable() +=("Using configuration", p)) render()

                            nl()

                            (configuration(p), p)
                        case None =>
                            return
                    }
                }

            open(cfg, cfgPath)
        }
        catch {
            case e: IgniteException =>
                warn(e.getMessage)
                warn("Type 'help open' to see how to use this command.")

                status("q")
        }
    }

    /**
     * Connects Visor console to configuration with path.
     *
     * @param cfg Configuration.
     * @param cfgPath Configuration path.
     */
    def open(cfg: IgniteConfiguration, cfgPath: String) {
        val daemon = Ignition.isDaemon

        val shutdownHook = IgniteSystemProperties.getString(IGNITE_NO_SHUTDOWN_HOOK, "false")

        // Make sure Visor console starts as daemon node.
        Ignition.setDaemon(true)

        // Make sure visor starts without shutdown hook.
        System.setProperty(IGNITE_NO_SHUTDOWN_HOOK, "true")

        val startedGridName = try {
             Ignition.start(cfg).name
        }
        finally {
            Ignition.setDaemon(daemon)

            System.setProperty(IGNITE_NO_SHUTDOWN_HOOK, shutdownHook)
        }

        this.cfgPath = cfgPath

        ignite =
            try
                Ignition.ignite(startedGridName).asInstanceOf[IgniteEx]
            catch {
                case _: IllegalStateException =>
                    this.cfgPath = null

                    throw new IgniteException("Named grid unavailable: " + startedGridName)
            }

        assert(cfgPath != null)

        isCon = true
        conOwner = true
        conTs = System.currentTimeMillis

        ignite.nodes().foreach(n => {
            setVarIfAbsent(nid8(n), "n")

            val ip = n.addresses().headOption

            if (ip.isDefined)
                setVarIfAbsent(ip.get, "h")
        })

        nodeJoinLsnr = new IgnitePredicate[Event]() {
            override def apply(e: Event): Boolean = {
                e match {
                    case de: DiscoveryEvent =>
                        setVarIfAbsent(nid8(de.eventNode()), "n")

                        val node = ignite.node(de.eventNode().id())

                        if (node != null) {
                            val ip = node.addresses().headOption

                            if (ip.isDefined)
                                setVarIfAbsent(ip.get, "h")
                        }
                        else {
                            warn(
                                "New node not found: " + de.eventNode().id(),
                                "Visor must have discovery configuration and local " +
                                    "host bindings identical with grid nodes."
                            )
                        }
                }

                true
            }
        }

        ignite.events().localListen(nodeJoinLsnr, EVT_NODE_JOINED)

        nodeLeftLsnr = new IgnitePredicate[Event]() {
            override def apply(e: Event): Boolean = {
                e match {
                    case (de: DiscoveryEvent) =>
                        val nv = mfind(nid8(de.eventNode()))

                        if (nv.isDefined)
                            mem.remove(nv.get._1)

                        val ip = de.eventNode().addresses.headOption

                        if (ip.isDefined) {
                            val last = !ignite.nodes().exists(n =>
                                n.addresses.size > 0 && n.addresses.head == ip.get
                            )

                            if (last) {
                                val hv = mfind(ip.get)

                                if (hv.isDefined)
                                    mem.remove(hv.get._1)
                            }
                        }
                }

                true
            }
        }

        ignite.events().localListen(nodeLeftLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED)

        nodeSegLsnr = new IgnitePredicate[Event] {
            override def apply(e: Event): Boolean = {
                e match {
                    case de: DiscoveryEvent =>
                        if (de.eventNode().id() == ignite.localNode.id) {
                            warn("Closing Visor console due to topology segmentation.")
                            warn("Contact your system administrator.")

                            nl()

                            close()
                        }
                }

                true
            }
        }

        ignite.events().localListen(nodeSegLsnr, EVT_NODE_SEGMENTED)

        nodeStopLsnr = new IgnitionListener {
            def onStateChange(name: String, state: IgniteState) {
                if (name == ignite.name && state == IgniteState.STOPPED) {
                    warn("Closing Visor console due to stopping of host grid instance.")

                    nl()

                    close()
                }
            }
        }

        Ignition.addListener(nodeStopLsnr)

        logText("Visor joined topology: " + cfgPath)
        logText("All live nodes, if any, will re-join.")

        nl()

        val t = VisorTextTable()

        // Print advise.
        println("Some useful commands:")

        t += ("Type 'top'", "to see full topology.")
        t += ("Type 'node'", "to see node statistics.")
        t += ("Type 'cache'", "to see cache statistics.")
        t += ("Type 'tasks'", "to see tasks statistics.")
        t += ("Type 'config'", "to see node configuration.")

        t.render()

        println("\nType 'help' to get help.\n")

        status()
    }

    /**
     * ==Command==
     * Connects Visor console to the default grid.
     *
     * ==Example==
     * <ex>open</ex>
     * Connects to the default grid.
     */
    def open() {
        open("")
    }

    /**
     * Returns string with node id8, its memory variable, if available, and its
     * IP address (first internal address), if node is alive.
     *
     * @param id Node ID.
     * @return String.
     */
    def nodeId8Addr(id: UUID): String = {
        assert(id != null)
        assert(isCon)

        val g = ignite

        if (g != null && g.localNode.id == id)
            "<visor>"
        else {
            val n = ignite.node(id)

            val id8 = nid8(id)
            val v = mfind(id8)

            id8 +
                (if (v.isDefined) "(@" + v.get._1 + ")" else "") +
                ", " +
                (if (n == null) "<n/a>" else n.addresses().headOption.getOrElse("<n/a>"))
        }
    }

    /**
     * Returns string with node id8 and its memory variable, if available.
     *
     * @param id Node ID.
     * @return String.
     */
    def nodeId8(id: UUID): String = {
        assert(id != null)
        assert(isCon)

        val id8 = nid8(id)
        val v = mfind(id8)

        id8 + (if (v.isDefined) "(@" + v.get._1 + ")" else "")
    }

    /**
     * Guards against invalid percent readings.
     *
     * @param v Value in '%' to guard. Any value below `0` and greater than `100`
     *      will return `n/a` string.
     */
    def safePercent(v: Double): String = {
        if (v < 0 || v > 100)
            "n/a"
        else
            formatDouble(v) + " %"
    }

    /** Convert to task argument. */
    def emptyTaskArgument[A](nid: UUID): VisorTaskArgument[Void] = new VisorTaskArgument(nid, false)

    def emptyTaskArgument[A](nids: Iterable[UUID]): VisorTaskArgument[Void] = new VisorTaskArgument(new JHashSet(nids),
      false)

    /** Convert to task argument. */
    def toTaskArgument[A](nid: UUID, arg: A): VisorTaskArgument[A] = new VisorTaskArgument(nid, arg, false)

    /** Convert to task argument. */
    def toTaskArgument[A](nids: Iterable[UUID], arg: A): VisorTaskArgument[A] = new VisorTaskArgument(
        new JHashSet(nids), arg, false)

    /**
     * Asks user to select a node from the list.
     *
     * @param title Title displayed before the list of nodes.
     * @return `Option` for ID of selected node.
     */
    def askForNode(title: String): Option[UUID] = {
        assert(title != null)
        assert(isCon)

        val t = VisorTextTable()

        t #= ("#", "Node ID8(@), IP", "Up Time", "CPUs", "CPU Load", "Free Heap")

        val nodes = ignite.nodes().toList

        if (nodes.isEmpty) {
            warn("Topology is empty.")

            None
        }
        else {
            (0 until nodes.size) foreach (i => {
                val n = nodes(i)

                val m = n.metrics

                val usdMem = m.getHeapMemoryUsed
                val maxMem = m.getHeapMemoryMaximum
                val freeHeapPct = (maxMem - usdMem) * 100 / maxMem

                val cpuLoadPct = m.getCurrentCpuLoad * 100

                t += (
                    i,
                    nodeId8Addr(n.id),
                    X.timeSpan2HMS(m.getUpTime),
                    n.metrics.getTotalCpus,
                    safePercent(cpuLoadPct),
                    formatDouble(freeHeapPct) + " %"
                )
            })

            println(title)

            t.render()

            val a = ask("\nChoose node number ('c' to cancel) [c]: ", "c")

            if (a.toLowerCase == "c")
                None
            else {
                try
                    Some(nodes(a.toInt).id)
                catch {
                    case e: Throwable =>
                        warn("Invalid selection: " + a)

                        None
                }
            }
        }
    }

    /**
     * Asks user to select a host from the list.
     *
     * @param title Title displayed before the list of hosts.
     * @return `Option` for projection of nodes located on selected host.
     */
    def askForHost(title: String): Option[ClusterGroup] = {
        assert(title != null)
        assert(isCon)

        val t = VisorTextTable()

        t #= ("#", "Int./Ext. IPs", "Node ID8(@)", "OS", "CPUs", "MACs", "CPU Load")

        val neighborhood = IgniteUtils.neighborhood(ignite.nodes()).values().toIndexedSeq

        if (neighborhood.isEmpty) {
            warn("Topology is empty.")

            None
        }
        else {
            (0 until neighborhood.size) foreach (i => {
                val neighbors = neighborhood(i)

                var ips = immutable.Set.empty[String]
                var id8s = Seq.empty[String]
                var macs = immutable.Set.empty[String]
                var cpuLoadSum = 0.0

                val n1 = neighbors.head

                assert(n1 != null)

                val cpus = n1.metrics.getTotalCpus

                val os = "" +
                    n1.attribute("os.name") + " " +
                    n1.attribute("os.arch") + " " +
                    n1.attribute("os.version")

                neighbors.foreach(n => {
                    id8s = id8s :+ nodeId8(n.id)

                    ips = ips ++ n.addresses()

                    cpuLoadSum += n.metrics().getCurrentCpuLoad

                    macs = macs ++ n.attribute[String](ATTR_MACS).split(", ").map(_.grouped(2).mkString(":"))
                })

                t += (
                    i,
                    ips.toSeq,
                    id8s,
                    os,
                    cpus,
                    macs.toSeq,
                    safePercent(cpuLoadSum / neighbors.size() * 100)
                )
            })

            println(title)

            t.render()

            val a = ask("\nChoose host number ('c' to cancel) [c]: ", "c")

            if (a.toLowerCase == "c")
                None
            else {
                try
                    Some(ignite.forNodes(neighborhood(a.toInt)))
                catch {
                    case e: Throwable =>
                        warn("Invalid selection: " + a)

                        None
                }
            }
        }
    }

    /**
     * Asks user to choose configuration file.
     *
     * @return `Option` for file path.
     */
    def askConfigFile(): Option[String] = {
        val files = GridConfigurationFinder.getConfigFiles

        if (files.isEmpty) {
            warn("No configuration files found.")

            None
        }
        else {
            val t = VisorTextTable()

            t #= ("#", "Configuration File")

            (0 until files.size).foreach(i => t += (i, files(i).get1()))

            println("Local configuration files:")

            t.render()

            val a = ask("\nChoose configuration file number ('c' to cancel) [0]: ", "0")

            if (a.toLowerCase == "c")
                None
            else {
                try
                    Some(files(a.toInt).get3.getPath)
                catch {
                    case e: Throwable =>
                        nl()

                        warn("Invalid selection: " + a)

                        None
                }
            }
        }
    }

    /**
     * Asks user input.
     *
     * @param prompt Prompt string.
     * @param dflt Default value for user input.
     * @param passwd If `true`, input will be masked with '*' character. `false` by default.
     */
    def ask(prompt: String, dflt: String, passwd: Boolean = false): String = {
        assert(prompt != null)
        assert(dflt != null)

        readLineOpt(prompt, if (passwd) Some('*') else None) match {
            case None => dflt
            case Some(s) if s.length == 0 => dflt
            case Some(s) => s
        }
    }

    /**
     * Safe `readLine` version.
     *
     * @param prompt User prompt.
     * @param mask Mask character (if `None`, no masking will be applied).
     */
    private def readLineOpt(prompt: String, mask: Option[Char]): Option[String] =
        try {
            val reader = new scala.tools.jline.console.ConsoleReader()

            val s = if (mask.isDefined)
                reader.readLine(prompt, mask.get)
            else
                reader.readLine(prompt)

            Option(s)
        }
        catch {
            case _: Throwable => None
        }

    /**
     * Asks user to choose node id8.
     *
     * @return `Option` for node id8.
     */
    def askNodeId(): Option[String] = {
        assert(isConnected)

        val ids = ignite.forRemotes().nodes().map(nid8).toList

        (0 until ids.size).foreach(i => println((i + 1) + ": " + ids(i)))

        println("\nC: Cancel")

        StdIn.readLine("\nChoose node: ") match {
            case "c" | "C" => None
            case idx =>
                try
                    Some(ids(idx.toInt - 1))
                catch {
                    case e: Throwable =>
                        if (idx.isEmpty)
                            warn("Index can't be empty.")
                        else
                            warn("Invalid index: " + idx + ".")

                        None
                }
        }
    }

    /**
     * Adds close callback. Added function will be called every time
     * command `close` is called.
     *
     * @param f Close callback to add.
     */
    def addShutdownCallback(f: () => Unit) {
        assert(f != null)

        shutdownCbs = shutdownCbs :+ f
    }

    /**
     * Adds close callback. Added function will be called every time
     * command `close` is called.
     *
     * @param f Close callback to add.
     */
    def addCloseCallback(f: () => Unit) {
        assert(f != null)

        cbs = cbs :+ f
    }

    /**
     * Removes close callback.
     *
     * @param f Close callback to remove.
     */
    def removeCloseCallback(f: () => Unit) {
        assert(f != null)

        cbs = cbs.filter(_ != f)
    }

    /**
     * Removes all close callbacks.
     */
    def removeCloseCallbacks() {
        cbs = Seq.empty[() => Unit]
    }

    /**
     * Gets visor uptime.
     */
    def uptime = if (isCon) System.currentTimeMillis() - conTs else -1L

    /**
     * ==Command==
     * Disconnects visor.
     *
     * ==Examples==
     * <ex>close</ex>
     * Disconnects from the grid.
     */
    def close() {
        if (!isConnected)
            adviseToConnect()
        else {
            if (pool != null) {
                pool.shutdown()

                try
                    if (!pool.awaitTermination(5, TimeUnit.SECONDS))
                        pool.shutdownNow
                catch {
                    case e: InterruptedException =>
                        pool.shutdownNow

                        Thread.currentThread.interrupt()
                }

                pool = new IgniteThreadPoolExecutor()
            }

            // Call all close callbacks.
            cbs foreach(_.apply())

            if (ignite != null && Ignition.state(ignite.name) == IgniteState.STARTED) {
                if (nodeJoinLsnr != null)
                    ignite.events().stopLocalListen(nodeJoinLsnr)

                if (nodeLeftLsnr != null)
                    ignite.events().stopLocalListen(nodeLeftLsnr)

                if (nodeSegLsnr != null)
                    ignite.events().stopLocalListen(nodeSegLsnr)
            }

            if (nodeStopLsnr != null)
                Ignition.removeListener(nodeStopLsnr)

            if (ignite != null && conOwner)
                try
                    Ignition.stop(ignite.name, true)
                catch {
                    case e: Exception => warn(e.getMessage)
                }

            // Fall through and treat Visor console as closed
            // even in case when grid didn't stop properly.

            logText("Visor left topology.")

            if (logStarted) {
                stopLog()

                nl()
            }

            isCon = false
            conOwner = false
            conTs = 0
            ignite = null
            nodeJoinLsnr = null
            nodeLeftLsnr = null
            nodeSegLsnr = null
            nodeStopLsnr = null
            cfgPath = null

            // Clear the memory.
            mclear()

            nl()

            status()
        }
    }

    /**
     * ==Command==
     * quit from Visor console.
     *
     * ==Examples==
     * <ex>quit</ex>
     * Quit from Visor console.
     */
    def quit() {
        System.exit(0)
    }

    /**
     * ==Command==
     * Prints log status.
     *
     * ==Examples==
     * <ex>log</ex>
     * Prints log status.
     */
    def log() {
        val t = VisorTextTable()

        t += ("Status", if (logStarted) "Started" else "Stopped")

        if (logStarted) {
            t += ("File path", logFile.getAbsolutePath)
            t += ("File size", if (logFile.exists) formatMemory(logFile.length()))
        }

        t.render()
    }

    /**
     * ==Command==
     * Starts or stops logging.
     *
     * ==Examples==
     * <ex>log -l -f=/home/user/visor-log</ex>
     * Starts logging to file `visor-log` located at `/home/user`.
     * <br>
     * <ex>log -l -f=log/visor-log</ex>
     * Starts logging to file `visor-log` located at &lt`Ignite home folder`&gt`/log`.
     * <br>
     * <ex>log -l -p=20</ex>
     * Starts logging with querying events period of 20 seconds.
     * <br>
     * <ex>log -l -t=30</ex>
     * Starts logging with topology snapshot logging period of 30 seconds.
     * <br>
     * <ex>log -s</ex>
     * Stops logging.
     *
     * @param args Command arguments.
     */
    def log(args: String) {
        assert(args != null)

        if (!isConnected)
            adviseToConnect()
        else {
            def scold(errMsgs: Any*) {
                assert(errMsgs != null)

                warn(errMsgs: _*)
                warn("Type 'help log' to see how to use this command.")
            }

            val argLst = parseArgs(args)

            if (hasArgFlag("s", argLst))
                if (!logStarted)
                    scold("Logging was not started.")
                else
                    stopLog()
            else if (hasArgFlag("l", argLst))
                if (logStarted)
                    scold("Logging is already started.")
                else
                    try
                        startLog(argValue("f", argLst), argValue("p", argLst), argValue("t", argLst),
                            hasArgFlag("dl", argLst))
                    catch {
                        case e: Exception => scold(e.getMessage)
                    }
            else
                scold("Invalid arguments.")
        }
    }

    /**
     * Stops logging.
     */
    private def stopLog() {
        assert(logStarted)

        logText("Log stopped.")

        if (logTimer != null) {
            logTimer.cancel()
            logTimer.purge()

            logTimer = null
        }

        if (topTimer != null) {
            topTimer.cancel()
            topTimer.purge()

            topTimer = null
        }

        logStarted = false

        println("<visor>: Log stopped: " + logFile.getAbsolutePath)
    }

    /**
     * Starts logging. If logging is already started - no-op.
     *
     * @param pathOpt `Option` for log file path. If `None` - default is used.
     * @param freqOpt `Option` for events fetching frequency If `None` - default is used.
     * @param topFreqOpt `Option` for topology refresh frequency.
     * @param rmtLogDisabled `True` if no events collected from remote nodes.
     */
    private def startLog(pathOpt: Option[String], freqOpt: Option[String], topFreqOpt: Option[String],
        rmtLogDisabled: Boolean) {
        assert(pathOpt != null)
        assert(freqOpt != null)
        assert(!logStarted)

        val path = pathOpt.getOrElse(DFLT_LOG_PATH)

        val f = new File(path)

        if (f.exists() && f.isDirectory)
            throw new IllegalArgumentException("Specified path is a folder. Please input valid file path.")

        val folder = Option(f.getParent).getOrElse("")
        val fileName = f.getName

        logFile = new File(IgniteUtils.resolveWorkDirectory(folder, false), fileName)

        logFile.createNewFile()

        if (!logFile.canWrite)
            throw new IllegalArgumentException("Not enough permissions to write a log file.")

        var freq = 0L

        try
            freq = freqOpt.getOrElse("10").toLong * 1000L
        catch {
            case e: NumberFormatException =>
                throw new IllegalArgumentException("Invalid frequency: " + freqOpt.get)
        }

        if (freq <= 0)
            throw new IllegalArgumentException("Frequency must be positive: " + freq)

        if (freq > 60000)
            warn("Frequency greater than a minute is too low (ignoring).")

        var topFreq = 0L

        try
            topFreq = topFreqOpt.getOrElse("20").toLong * 1000L
        catch {
            case e: NumberFormatException =>
                throw new IllegalArgumentException("Invalid topology frequency: " + topFreqOpt.get)
        }

        if (topFreq <= 0)
            throw new IllegalArgumentException("Topology frequency must be positive: " + topFreq)

        // Unique key for this JVM.
        val key = UUID.randomUUID().toString + System.identityHashCode(classOf[java.lang.Object]).toString

        logTimer = new Timer(true)

        logTimer.schedule(new TimerTask() {
            /** Events to be logged by Visor console (additionally to discovery events). */
            private final val LOG_EVTS = Array(
                EVT_JOB_TIMEDOUT,
                EVT_JOB_FAILED,
                EVT_JOB_FAILED_OVER,
                EVT_JOB_REJECTED,
                EVT_JOB_CANCELLED,

                EVT_TASK_TIMEDOUT,
                EVT_TASK_FAILED,
                EVT_TASK_DEPLOY_FAILED,
                EVT_TASK_DEPLOYED,
                EVT_TASK_UNDEPLOYED,

                EVT_LIC_CLEARED,
                EVT_LIC_VIOLATION,
                EVT_LIC_GRACE_EXPIRED,

                EVT_CACHE_PRELOAD_STARTED,
                EVT_CACHE_PRELOAD_STOPPED,
                EVT_CLASS_DEPLOY_FAILED
            )

            override def run() {
                val g = ignite

                if (g != null) {
                    try {
                        // Discovery events collected only locally.
                        val loc = g.compute(g.forLocal()).withName("visor-log-collector").withNoFailover().
                            execute(classOf[VisorNodeEventsCollectorTask], toTaskArgument(g.localNode().id(),
                            VisorNodeEventsCollectorTaskArg.createLogArg(key, LOG_EVTS ++ EVTS_DISCOVERY))).toSeq

                        val evts = if (!rmtLogDisabled) {
                            val prj = g.forRemotes()

                            loc ++ g.compute(prj).withName("visor-log-collector").withNoFailover().
                                execute(classOf[VisorNodeEventsCollectorTask], toTaskArgument(prj.nodes().map(_.id()),
                                    VisorNodeEventsCollectorTaskArg.createLogArg(key, LOG_EVTS))).toSeq
                        }
                        else
                            loc

                        if (evts.nonEmpty) {
                            var out: FileWriter = null

                            try {
                                out = new FileWriter(logFile, true)

                                evts.toList.sortBy(_.timestamp).foreach(e => {
                                    logImpl(
                                        out,
                                        formatDateTime(e.timestamp),
                                        nodeId8Addr(e.nid()),
                                        IgniteUtils.compact(e.shortDisplay())
                                    )

                                    if (EVTS_DISCOVERY.contains(e.typeId()))
                                        snapshot()
                                })
                            }
                            finally {
                                IgniteUtils.close(out, null)
                            }
                        }
                    }
                    catch {
                        case _: ClusterGroupEmptyCheckedException => // Ignore.
                        case e: Exception => logText("Failed to collect log.")
                    }
                }
            }
        }, freq, freq)

        topTimer = new Timer(true)

        topTimer.schedule(new TimerTask() {
            override def run() {
                snapshot()
            }
        }, topFreq, topFreq)

        logStarted = true

        logText("Log started.")

        println("<visor>: Log started: " + logFile.getAbsolutePath)
    }

    /**
     * Does topology snapshot.
     */
    private def snapshot() {
        val g = ignite

        if (g != null)
            try
                drawBar(g.metrics())
            catch {
                case e: ClusterGroupEmptyCheckedException => logText("Topology is empty.")
                case e: Exception => ()
            }
    }

    /**
     *
     * @param m Projection metrics.
     */
    private def drawBar(m: ClusterMetrics) {
        assert(m != null)

        val pipe = "|"

        def bar(cpuLoad: Double, memUsed: Double): String = {
            val nCpu = if (cpuLoad < 0 || cpuLoad > 1) 0 else (cpuLoad * 20).toInt
            val nMem = if (memUsed < 0 || memUsed > 1) 0 else (memUsed * 20).toInt

            ("" /: (0 until 20))((s: String, i: Int) => {
                s + (i match {
                    case a if a == nMem => "^"
                    case a if a <= nCpu => "="
                    case _ => '.'
                })
            })
        }

        logText("H/N/C" + pipe +
            IgniteUtils.neighborhood(ignite.nodes()).size.toString.padTo(4, ' ') + pipe +
            ignite.nodes().size().toString.padTo(4, ' ') + pipe +
            m.getTotalCpus.toString.padTo(4, ' ') + pipe +
            bar(m.getAverageCpuLoad, m.getHeapMemoryUsed / m.getHeapMemoryMaximum) + pipe
        )
    }

    /**
     * Logs text message.
     *
     * @param msg Message to log.
     */
    def logText(msg: String) {
        assert(msg != null)

        if (logStarted) {
            var out: FileWriter = null

            try {
                out = new FileWriter(logFile, true)

                logImpl(
                    out,
                    formatDateTime(System.currentTimeMillis),
                    null,
                    msg
                )
            }
            catch {
                case e: IOException => ()
            }
            finally {
                IgniteUtils.close(out, null)
            }
        }
    }

    /**
     * @param out Writer.
     * @param tstamp Timestamp of the log.
     * @param node Node associated with the event.
     * @param msg Message associated with the event.
     */
    private def logImpl(
        out: java.io.Writer,
        tstamp: String,
        node: String = null,
        msg: String
    ) {
        assert(out != null)
        assert(tstamp != null)
        assert(msg != null)
        assert(logStarted)

        if (node != null)
            out.write(tstamp.padTo(18, ' ') + " | " + node + " => " + msg + "\n")
        else
            out.write(tstamp.padTo(18, ' ') + " | " + msg + "\n")
    }

    /**
     * Prints out status and help in case someone calls `visor()`.
     *
     */
    def apply() {
        status()

        nl()

        help()
    }

    lazy val commands = cmdLst.map(_.name) ++ cmdLst.map(_.aliases).flatten

    def searchCmd(cmd: String) = cmdLst.find(c => c.name.equals(cmd) || (c.aliases != null && c.aliases.contains(cmd)))

    /**
     * Transform node ID to ID8 string.
     *
     * @param node Node to take ID from.
     * @return Node ID in ID8 format.
     */
    def nid8(node: ClusterNode): String = {
        nid8(node.id())
    }

    /**
     * Transform node ID to ID8 string.
     *
     * @param nid Node ID.
     * @return Node ID in ID8 format.
     */
    def nid8(nid: UUID): String = {
        nid.toString.take(8).toUpperCase
    }

    /**
     * Get node by ID8 string.
     *
     * @param id8 Node ID in ID8 format.
     * @return Collection of nodes that has specified ID8.
     */
    def nodeById8(id8: String) = {
        ignite.nodes().filter(n => id8.equalsIgnoreCase(nid8(n)))
    }

    /**
     * Introduction of `^^` operator for `Any` type that will call `break`.
     *
     * @param v `Any` value.
     */
    implicit def toReturnable(v: Any) = new {
        // Ignore the warning below.
        def ^^ {
            break()
        }
    }

    /**
     * Decode time frame from string.
     *
     * @param timeArg Optional time frame: &lt;num&gt;s|m|h|d
     * @return Time in milliseconds.
     */
    def timeFilter(timeArg: Option[String]): Long = {
        if (timeArg.nonEmpty) {
            val s = timeArg.get

            val n = try
                s.substring(0, s.length - 1).toLong
            catch {
                case _: NumberFormatException =>
                    throw new IllegalArgumentException("Time frame size is not numeric in: " + s)
            }

            if (n <= 0)
                throw new IllegalArgumentException("Time frame size is not positive in: " + s)

            val timeUnit = s.last match {
                case 's' => 1000L
                case 'm' => 1000L * 60L
                case 'h' => 1000L * 60L * 60L
                case 'd' => 1000L * 60L * 60L * 24L
                case _ => throw new IllegalArgumentException("Invalid time frame suffix in: " + s)
            }

            n * timeUnit
        }
        else
            Long.MaxValue
    }
}
