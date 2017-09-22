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

package org.apache.ignite.visor.commands

import org.apache.ignite.IgniteSystemProperties._
import org.apache.ignite.internal.IgniteVersionUtils._
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.startup.cmdline.AboutDialog
import org.apache.ignite.visor.visor
import org.apache.ignite.visor.visor._

import jline.TerminalSupport
import jline.console.ConsoleReader
import jline.console.completer.Completer
import jline.internal.Configuration

import javax.swing.ImageIcon
import java._
import java.awt.Image
import java.io._
import java.text.SimpleDateFormat

import scala.io._

/**
 * Command line Visor.
 */
class VisorConsole {
    /** Version number. */
    protected def version() = VER_STR

    /** Copyright. */
    protected def copyright() = COPYRIGHT

    /** Release date. */
    protected def releaseDate() = new SimpleDateFormat("ddMMyyyy").parse(RELEASE_DATE_STR)

    /** Program name. */
    protected val progName = sys.props.getOrElse(IGNITE_PROG_NAME, "ignitevisorcmd")

    /**
     * Built-in commands.
     */
    protected def addCommands() {
        // Note the importing of implicit conversions.
        org.apache.ignite.visor.commands.ack.VisorAckCommand
        org.apache.ignite.visor.commands.alert.VisorAlertCommand
        org.apache.ignite.visor.commands.cache.VisorCacheClearCommand
        org.apache.ignite.visor.commands.cache.VisorCacheCommand
        org.apache.ignite.visor.commands.cache.VisorCacheSwapCommand
        org.apache.ignite.visor.commands.config.VisorConfigurationCommand
        org.apache.ignite.visor.commands.deploy.VisorDeployCommand
        org.apache.ignite.visor.commands.disco.VisorDiscoveryCommand
        org.apache.ignite.visor.commands.events.VisorEventsCommand
        org.apache.ignite.visor.commands.gc.VisorGcCommand
        org.apache.ignite.visor.commands.kill.VisorKillCommand
        org.apache.ignite.visor.commands.node.VisorNodeCommand
        org.apache.ignite.visor.commands.open.VisorOpenCommand
        org.apache.ignite.visor.commands.ping.VisorPingCommand
        org.apache.ignite.visor.commands.start.VisorStartCommand
        org.apache.ignite.visor.commands.tasks.VisorTasksCommand
        org.apache.ignite.visor.commands.top.VisorTopologyCommand
        org.apache.ignite.visor.commands.vvm.VisorVvmCommand
    }

    protected def parse(args: String) = {
        val argLst = parseArgs(args)

        if (hasArgFlag("?", argLst) || hasArgFlag("help", argLst)) {
            println("Usage:")
            println(s"    $progName [? | -help]|[{-v}{-np} {-cfg=<path>}]|[{-b=<path>} {-e=command1;command2;...}]")
            println("    Where:")
            println("        ?, /help, -help      - show this message.")
            println("        -v                   - verbose mode (quiet by default).")
            println("        -np                  - no pause on exit (pause by default).")
            println("        -cfg=<path>          - connect with specified configuration.")
            println("        -b=<path>            - batch mode with file.")
            println("        -e=cmd1;cmd2;...     - batch mode with commands.")

            visor.quit()
        }

        argLst
    }

    protected def buildReader(argLst: ArgList) = {
        val cfgFile = argValue("cfg", argLst)
        val batchFile = argValue("b", argLst)
        val batchCommand = argValue("e", argLst)

        cfgFile.foreach(cfg => {
            if (cfg.trim.isEmpty) {
                visor.warn("Expected path to configuration after \"-cfg\" option.")

                visor.quit()
            }

            if (batchFile.isDefined || batchCommand.isDefined) {
                visor.warn("Options can't contains both -cfg and one of -b or -e options.")

                visor.quit()
            }

            visor.searchCmd("open").foreach(_.withArgs("-cpath=" + cfg))
        })

        if (batchFile.isDefined && batchCommand.isDefined) {
            visor.warn("Options can't contains both command file and commands.")

            visor.quit()
        }

        var batchStream: Option[String] = None

        batchFile.foreach(name => {
            val f = U.resolveIgnitePath(name)

            if (f == null) {
                visor.warn(
                    "Can't find batch commands file: " + name,
                    s"Usage: $progName {-b=<batch command file path>} {-e=command1;command2}"
                )

                visor.quit()
            }

            batchStream = Some(Source.fromFile(f).getLines().mkString("\n"))
        })

        batchCommand.foreach(commands => batchStream = Some(commands.replaceAll(";", "\n")))

        val inputStream = batchStream match {
            case Some(cmd) =>
                visor.batchMode = true

                new ByteArrayInputStream((cmd + "\nquit\n").getBytes("UTF-8"))
            case None => new FileInputStream(FileDescriptor.in)
        }

        // Workaround for IDEA terminal.
        val term = try {
            Class.forName("com.intellij.rt.execution.application.AppMain")

            new TerminalSupport(false) {}
        } catch {
            case ignored: ClassNotFoundException => null
        }

        val reader = new ConsoleReader(inputStream, System.out, term)

        reader.addCompleter(new VisorCommandCompleter(visor.commands))
        reader.addCompleter(new VisorFileNameCompleter())

        reader
    }

    protected def mainLoop(reader: ConsoleReader) {
        welcomeMessage()

        var ok = true

        // Wrap line symbol for user input.
        val wrapLine = if (U.isWindows) "^" else "\\"

        val emptyArg = "^([a-zA-z!?]+)$".r
        val varArg = "^([a-zA-z!?]+)\\s+(.+)$".r

        var line: String = null

        val buf = new StringBuilder

        while (ok) {
            line = reader.readLine("visor> ")

            ok = line != null

            if (ok) {
                line = line.trim

                if (line.endsWith(wrapLine)) {
                    buf.append(line.dropRight(1))
                }
                else {
                    if (buf.nonEmpty) {
                        buf.append(line)

                        line = buf.toString()

                        buf.clear()
                    }

                    try {
                        line match {
                            case emptyArg(c) =>
                                visor.searchCmd(c) match {
                                    case Some(cmdHolder) => cmdHolder.emptyArgs()
                                    case _ => adviseToHelp(c)
                                }
                            case varArg(c, a) =>
                                visor.searchCmd(c) match {
                                    case Some(cmdHolder) => cmdHolder.withArgs(a.trim)
                                    case _ => adviseToHelp(c)
                                }
                            case s if "".equals(s.trim) => // Ignore empty user input.
                            case _ => adviseToHelp(line)
                        }
                    } catch {
                        case ignore: Exception =>
                            ignore.printStackTrace()

                            adviseToHelp(line)
                    }
                }
            }
        }
    }

    /**
     * Prints standard 'Invalid command' error message.
     */
    protected def adviseToHelp(input: String) {
        visor.warn(
            "Invalid command name: '" + input + "'",
            "Type 'help' to print commands list."
        )
    }

    /**
     * Print banner, hint message on start.
     */
    protected def welcomeMessage() {
        println("___    _________________________ ________" +  NL +
                "__ |  / /____  _/__  ___/__  __ \\___  __ \\" +  NL +
                "__ | / /  __  /  _____ \\ _  / / /__  /_/ /" +  NL +
                "__ |/ /  __/ /   ____/ / / /_/ / _  _, _/" +  NL +
                "_____/   /___/   /____/  \\____/  /_/ |_|" +  NL +
                NL +
                "ADMIN CONSOLE" + NL +
                copyright())

        nl()

        status()

        println("\nType 'help' for more information.")
        println("Type 'open' to join the grid.")
        println("Type 'quit' to quit form Visor console.")

        nl()
    }

    /**
     * Setting up Mac OS specific system menu.
     */
    protected def addAboutDialog() {
        def urlIcon(iconName: String) = {
            val iconPath = "org/apache/ignite/startup/cmdline/" + iconName

            val dockIconUrl = U.detectClassLoader(getClass).getResource(iconPath)

            assert(dockIconUrl != null, "Unknown icon path: " + iconPath)

            dockIconUrl
        }

        try {
            val appCls = Class.forName("com.apple.eawt.Application")
            val aboutHndCls = Class.forName("com.apple.eawt.AboutHandler")

            val osxApp = appCls.getDeclaredMethod("getApplication").invoke(null)

            val dockIco = new ImageIcon(urlIcon("logo_ignite_128x128.png"))

            appCls.getDeclaredMethod("setDockIconImage", classOf[Image]).invoke(osxApp, dockIco.getImage)

            val bannerIconUrl = urlIcon("logo_ignite_48x48.png")

            val aboutHndProxy = java.lang.reflect.Proxy.newProxyInstance(
                appCls.getClassLoader,
                Array[Class[_]](aboutHndCls),
                new java.lang.reflect.InvocationHandler {
                    def invoke(proxy: Any, mth: java.lang.reflect.Method, args: Array[Object]) = {
                        AboutDialog.centerShow("Visor - Ignite Shell Console", bannerIconUrl.toExternalForm,
                            version(), releaseDate(), copyright())

                        null
                    }
                })

            appCls.getDeclaredMethod("setAboutHandler", aboutHndCls).invoke(osxApp, aboutHndProxy)
        }
        catch {
            // Specifically ignore it here.
            case _: Throwable =>
        }
    }
}

/**
 * Command line Visor entry point.
 */
object VisorConsole extends VisorConsole with App {
    addAboutDialog()

    addCommands()

    private val argLst = parse(args.mkString(" "))

    private val reader = buildReader(argLst)

    visor.reader(reader)

    mainLoop(reader)
}

/**
 * Visor command list completer.
 *
 * @param commands Commands list.
 */
private[commands] class VisorCommandCompleter(commands: Seq[String]) extends Completer {
    import scala.collection.JavaConversions._

    /** ordered commands. */
    private final val strings = new util.TreeSet[String](commands)

    @impl def complete(buf: String, cursor: Int, candidates: util.List[CharSequence]): Int = {
        // buffer could be null
        assert(candidates != null)

        if (buf == null)
            candidates.addAll(strings)
        else
            strings.tailSet(buf).takeWhile(_.startsWith(buf)).foreach(candidates.add)

        if (candidates.size == 1)
            candidates.set(0, candidates.get(0) + " ")

        if (candidates.isEmpty) -1 else 0
    }
}

/**
 * File path completer for different place of path in command.
 */
private[commands] class VisorFileNameCompleter extends Completer {
    protected lazy val getUserHome = Configuration.getUserHome

    protected lazy val separator = File.separator

    @impl def complete(buf: String, cursor: Int, candidates: util.List[CharSequence]): Int = {
        assert(candidates != null)

        var ixBegin = 0

        // extracted path from buffer.
        val path = buf match {
            case null => ""
            case emptyStr if emptyStr.trim == "" => ""
            case str =>
                // replace wrong '/' on windows.
                val translated = if (U.isWindows) str.replace('/', '\\') else str

                // line before cursor.
                val left = translated.substring(0, cursor)

                // path begin marker.
                val quote = if (left.count(_ == '\"') % 2 == 1) "\""
                    else if (left.count(_ == '\'') % 2 == 1) "\'"
                    else ""

                val splitterSz = quote.length + " ".length

                // path begin marker index.
                ixBegin = left.lastIndexOf(" " + quote)
                ixBegin = if (ixBegin != -1) ixBegin + splitterSz else left.length - 1

                // path end marker index.
                var ixEnd = translated.indexOf(quote + " ", cursor)
                ixEnd = if (ixEnd != -1) ixEnd - splitterSz else translated.length

                // extract path.
                translated.substring(ixBegin, ixEnd)
        }

        // resolve path
        val file = resolvePath(path)

        // file dir and part of file name for complete.
        val (dir, partOfName) = if (file.isDirectory) (file, "") else (file.getParentFile, file.getName)

        // filter all files in directory by part of file name.
        if (dir != null && dir.listFiles != null) {
            val files = for (file <- dir.listFiles if file.getName.startsWith(partOfName)) yield file

            if (files.length == 1) {
                val candidate = files(0)

                candidates.add(candidate.getName + (if (candidate.isDirectory) separator else " "))
            }
            else
                files.foreach(f => candidates.add(f.getName))
        }

        if (candidates.size > 0) ixBegin + path.lastIndexOf(separator) + separator.length else -1
    }

    /**
     * Gets File representing the path passed in. First the check is made if path is in user home directory.
     * If not, then the check is if path is absolute.
     * If all checks fail, then related to the current dir File is returned.
     *
     * @param path - Path to resolve.
     * @return Resolved path as File
     */
    protected def resolvePath(path: String) = {
        val homeDir = getUserHome

        val absFile = new File(path)

        // Special character: ~ maps to the user's home directory
        if (path.startsWith("~" + separator))
            new File(homeDir.getPath, path.substring(1))
        else if (path.equals("~"))
            homeDir.getParentFile
        else if (absFile.exists() || absFile.getParentFile != null) // absolute path
            absFile
        else
            new File(new File("").getAbsolutePath, path)
    }
}
