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

import org.apache.ignite.internal.IgniteVersionUtils._
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.util.{IgniteUtils => U}
import org.apache.ignite.startup.cmdline.AboutDialog

import javax.swing.ImageIcon
import java.awt.Image
import java.io.File
import java.text.SimpleDateFormat
import java.util

// Built-in commands.
// Note the importing of implicit conversions.
import org.apache.ignite.visor.commands.ack.VisorAckCommand
import org.apache.ignite.visor.commands.alert.VisorAlertCommand
import org.apache.ignite.visor.commands.cache.{VisorCacheClearCommand, VisorCacheCommand, VisorCacheCompactCommand, VisorCacheSwapCommand}
import org.apache.ignite.visor.commands.config.VisorConfigurationCommand
import org.apache.ignite.visor.commands.deploy.VisorDeployCommand
import org.apache.ignite.visor.commands.disco.VisorDiscoveryCommand
import org.apache.ignite.visor.commands.events.VisorEventsCommand
import org.apache.ignite.visor.commands.gc.VisorGcCommand
import org.apache.ignite.visor.commands.kill.VisorKillCommand
import org.apache.ignite.visor.commands.node.VisorNodeCommand
import org.apache.ignite.visor.commands.ping.VisorPingCommand
import org.apache.ignite.visor.commands.start.VisorStartCommand
import org.apache.ignite.visor.commands.tasks.VisorTasksCommand
import org.apache.ignite.visor.commands.top.VisorTopologyCommand
import org.apache.ignite.visor.commands.vvm.VisorVvmCommand
import org.apache.ignite.visor.visor

import scala.tools.jline.console.ConsoleReader
import scala.tools.jline.console.completer.Completer
import scala.tools.jline.internal.Configuration

/**
 * Command line Visor.
 */
object VisorConsole extends App {
    /** Version number. */
    private final val VISOR_VER = VER_STR

    /** Release date. */
    private final val VISOR_RELEASE_DATE = RELEASE_DATE_STR

    /** Copyright. */
    private final val VISOR_COPYRIGHT = COPYRIGHT

    /** Release date (another format). */
    private final val releaseDate = new SimpleDateFormat("ddMMyyyy").parse(VISOR_RELEASE_DATE)

    // Pre-initialize built-in commands.
    VisorAckCommand
    VisorAlertCommand
    VisorCacheCommand
    VisorCacheClearCommand
    VisorCacheCompactCommand
    VisorCacheSwapCommand
    VisorConfigurationCommand
    VisorDeployCommand
    VisorDiscoveryCommand
    VisorEventsCommand
    VisorGcCommand
    VisorKillCommand
    VisorNodeCommand
    VisorPingCommand
    VisorTopologyCommand
    VisorStartCommand
    VisorTasksCommand
    VisorVvmCommand

    // Setting up Mac OS specific system menu.
    customizeUI()

    // Wrap line symbol for user input.
    private val wrapLine = if (U.isWindows) "^" else "\\"

    private val emptyArg = "^([a-zA-z!?]+)$".r
    private val varArg = "^([a-zA-z!?]+)\\s+(.+)$".r

    private var line: String = null

    private val buf = new StringBuilder

    private val reader = new ConsoleReader()

    reader.addCompleter(new VisorCommandCompleter(visor.commands))
    reader.addCompleter(new VisorFileNameCompleter())

    welcomeMessage()

    private var ok = true

    while (ok) {
        line = reader.readLine("visor> ")

        ok = line != null

        if (ok) {
            line = line.trim

            if (line.endsWith(wrapLine)) {
                buf.append(line.dropRight(1))
            }
            else {
                if (buf.size != 0) {
                    buf.append(line)

                    line = buf.toString()

                    buf.clear()
                }

                try {
                    line match {
                        case emptyArg(c) =>
                            visor.searchCmd(c) match {
                                case Some(cmdHolder) => cmdHolder.impl.invoke()
                                case _ => adviseToHelp(c)
                            }
                        case varArg(c, args) =>
                            visor.searchCmd(c) match {
                                case Some(cmdHolder) => cmdHolder.impl.invoke(args.trim)
                                case _ => adviseToHelp(c)
                            }
                        case s if "".equals(s.trim) => // Ignore empty user input.
                        case _ => adviseToHelp(line)
                    }
                } catch {
                    case ignore: Exception => ignore.printStackTrace()
                }
            }
        }
    }

    def terminalWidth() = reader.getTerminal.getWidth

    /**
     * Prints standard 'Invalid command' error message.
     */
    private def adviseToHelp(input: String) {
        visor.warn(
            "Invalid command name: '" + input + "'",
            "Type 'help' to print commands list."
        )
    }

    /**
     * Print banner, hint message on start.
     */
    private def welcomeMessage() {
        visor.status()

        println("\nType 'help' for more information.")
        println("Type 'open' to join the grid.")
        println("Type 'quit' to quit form Visor console.")

        visor.nl()
    }

    /**
     * Setting up mac os specific menu.
     */
    private def customizeUI() {
        def urlIcon(iconPath: String) = {
            val dockIconUrl = getClass.getResource(iconPath)

            assert(dockIconUrl != null, "Unknown icon path: " + iconPath)

            dockIconUrl
        }

        try {
            val appCls = Class.forName("com.apple.eawt.Application")
            val aboutHndCls = Class.forName("com.apple.eawt.AboutHandler")

            val osxApp = appCls.getDeclaredMethod("getApplication").invoke(null)

            val dockIco = new ImageIcon(urlIcon("ggcube_node_128x128.png"))

            appCls.getDeclaredMethod("setDockIconImage", classOf[Image]).invoke(osxApp, dockIco.getImage)

            val bannerIconUrl = urlIcon("ggcube_node_48x48.png")

            val aboutHndProxy = java.lang.reflect.Proxy.newProxyInstance(
                appCls.getClassLoader,
                Array[Class[_]](aboutHndCls),
                new java.lang.reflect.InvocationHandler {
                    def invoke(proxy: Any, mth: java.lang.reflect.Method, args: Array[Object]) = {
                        AboutDialog.centerShow("Visor - Ignite Shell Console", bannerIconUrl.toExternalForm,
                            VISOR_VER, releaseDate, VISOR_COPYRIGHT)

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

                val splitterSz = quote.size + " ".size

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

            if (files.size == 1) {
                val candidate = files(0)

                if (candidate.isDirectory) separator else " "

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
