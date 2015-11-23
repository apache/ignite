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

package org.apache.ignite.visor.commands.kill

import org.apache.ignite._
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.IgniteNodeAttributes._
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.common.VisorConsoleCommand
import org.apache.ignite.visor.visor._

import java.util.{Collections, UUID}

import scala.collection.JavaConversions._
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Contains Visor command `kill` implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | kill | Kills or restarts node. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     kill
 *     kill "-in|-ih"
 *     kill "{-r|-k} {-id8=<node-id8>|-id=<node-id>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -in
 *         Run command in interactive mode with ability to
 *         choose a node to kill or restart.
 *         Note that either '-in' or '-ih' can be specified.
 *
 *         This mode is used by default.
 *     -ih
 *         Run command in interactive mode with ability to
 *         choose a host where to kill or restart nodes.
 *         Note that either '-in' or '-ih' can be specified.
 *     -r
 *         Restart node mode.
 *         Note that either '-r' or '-k' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -k
 *         Kill (stop) node mode.
 *         Note that either '-r' or '-k' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -id8=<node-id8>
 *         ID8 of the node to kill or restart.
 *         Note that either '-id8' or '-id' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -id=<node-id>
 *         ID of the node to kill or restart.
 *         Note that either '-id8' or '-id' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 * }}}
 *
 * ====Examples====
 * {{{
 *     kill
 *         Starts command in interactive mode.
 *     kill "-id8=12345678 -r"
 *         Restart node with '12345678' ID8.
 *     kill "-k"
 *         Kill (stop) all nodes.
 * }}}
 */
class VisorKillCommand extends VisorConsoleCommand {
    @impl protected val name = "kill"

    /**
     * ===Command===
     * Stops or restarts a JVM indicated by the node ID.
     *
     * ===Examples===
     * <ex>kill "-id8=12345678 -r"<ex>
     * Restarts the specified node.
     *
     * <ex>kill "-k"</ex>
     * Stops all nodes.
     *
     * @param args Command arguments.
     */
    def kill(args: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val iNodes = hasArgFlag("in", argLst)
            val iHosts = hasArgFlag("ih", argLst)

            if (iNodes && iHosts)
                scold("Only one of '-in' or '-ih' can be specified.").^^
            else if (iNodes)
                interactiveNodes().^^
            else if (iHosts)
                interactiveHosts().^^

            val id8 = argValue("id8", argLst)
            val id = argValue("id", argLst)
            val restart = hasArgFlag("r", argLst)
            val kill = hasArgFlag("k", argLst)

            var node: ClusterNode = null

            if (kill && restart)
                scold("Only one of '-k' or '-r' can be specified.")
            else if (!kill && !restart)
                scold("Missing '-k' or '-r' option in command: " + args)
            else if (id8.isDefined && id.isDefined)
                scold("Only one of -id8 or -id is allowed.")
            else {
                if (id8.isDefined) {
                    val ns = nodeById8(id8.get)

                    if (ns.isEmpty)
                        scold("Unknown 'id8' value: " + id8.get).^^
                    else if (ns.size != 1) {
                        scold("'id8' resolves to more than one node (use full 'id' instead) : " + args).^^
                    }
                    else
                        node = ns.head
                }
                else if (id.isDefined)
                    try {
                        node = ignite.cluster.node(java.util.UUID.fromString(id.get))

                        if (node == null)
                            scold("'id' does not match any node : " + args).^^
                    }
                    catch {
                        case e: IllegalArgumentException => scold("Invalid node 'id' in args: " + args).^^
                    }

                if (node == null && (id.isDefined || id8.isDefined))
                    scold("Node with given ID cannot be found.").^^

                try
                    // In case of the restart - check that target node supports it.
                    if (restart && node != null && node.attribute[String](ATTR_RESTART_ENABLED) != "true")
                        scold("Node doesn't support restart: " + nid8(node)).^^
                catch {
                    case e: IgniteException => scold("Failed to restart the node. " + e.getMessage).^^
                }

                val op = if (restart) "restart" else "kill"

                try
                    killOrRestart(if (node == null) ignite.cluster.nodes().map(_.id()) else Collections.singleton(node.id()), restart)
                catch {
                    case _: IgniteException => scold("Failed to " + op + " due to system error.").^^
                }
            }
        }
    }

    /**
     * ===Command===
     * Run command in interactive mode.
     *
     * ===Examples===
     * <ex>kill</ex>
     * Starts command in interactive mode.
     */
    def kill() {
        kill("-in")
    }

    /**
     * Kills or restarts nodes in provided projection.
     *
     * @param nodes Projection.
     * @param restart Restart flag.
     */
    private def killOrRestart(nodes: Iterable[UUID], restart: Boolean) {
        assert(nodes != null)

        if (nodes.isEmpty)
            warn("Topology is empty.").^^

        val op = if (restart) "restart" else "kill"

        if (nodes.size == ignite.cluster.nodes().size())
            ask("Are you sure you want to " + op + " ALL nodes? (y/n) [n]: ", "n") match {
                case "y" | "Y" =>  ask("You are about to " + op + " ALL nodes. " +
                    "Are you 100% sure? (y/n) [n]: ", "n") match {
                        case "y" | "Y" => ()
                        case "n" | "N" => break()
                        case x => nl(); warn("Invalid answer: " + x); break()
                    }
                case "n" | "N" => break()
                case x => nl(); warn("Invalid answer: " + x); break()
            }
        else if (nodes.size > 1)
            ask("Are you sure you want to " + op + " several nodes? (y/n) [n]: ", "n") match {
                case "y" | "Y" => ()
                case "n" | "N" => break()
                case x => nl(); warn("Invalid answer: " + x); break()
            }
        else
            ask("Are you sure you want to " + op + " this node? (y/n) [n]: ", "n") match {
                case "y" | "Y" => ()
                case "n" | "N" => break()
                case x => nl(); warn("Invalid answer: " + x); break()
            }

        if (restart)
            ignite.cluster.restartNodes(nodes)
        else
            ignite.cluster.stopNodes(nodes)
    }

    /**
     * Runs interactive mode of the command (choosing node).
     */
    private def interactiveNodes() {
        askForNode("Select node from:") match {
            case Some(id) => ask("Do you want to [k]ill or [r]estart? (k/r) [r]: ", "r") match {
                case "k" | "K" => killOrRestart(Seq(id), false)
                case "r" | "R" => killOrRestart(Seq(id), true)
                case x => nl(); warn("Invalid answer: " + x)
            }
            case None => ()
        }
    }

    /**
     * Runs interactive mode of the command (choosing host).
     */
    private def interactiveHosts() {
        askForHost("Select host from:") match {
            case Some(p) => ask("Do you want to [k]ill or [r]estart? (k/r) [r]: ", "r") match {
                case "k" | "K" => killOrRestart(p.nodes().map(_.id), false)
                case "r" | "R" => killOrRestart(p.nodes().map(_.id), true)
                case x => nl(); warn("Invalid answer: " + x)
            }
            case None => ()
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorKillCommand {
    /** Singleton command. */
    private val cmd = new VisorKillCommand

    // Adds command's help to visor.
    addHelp(
        name = cmd.name,
        shortInfo = "Kills or restarts node.",
        spec = List(
            cmd.name,
            s"${cmd.name} -in|-ih",
            s"${cmd.name} {-r|-k} {-id8=<node-id8>|-id=<node-id>}"
        ),
        args = List(
            "-in" -> List(
                "Run command in interactive mode with ability to",
                "choose a node to kill or restart.",
                "Note that either '-in' or '-ih' can be specified.",
                " ",
                "This mode is used by default."
            ),
            "-ih" -> List(
                "Run command in interactive mode with ability to",
                "choose a host where to kill or restart nodes.",
                "Note that either '-in' or '-ih' can be specified."
            ),
            "-r" -> List(
                "Restart node mode.",
                "Note that either '-r' or '-k' can be specified.",
                "If no parameters provided - command starts in interactive mode."
            ),
            "-k" -> List(
                "Kill (stop) node mode.",
                "Note that either '-r' or '-k' can be specified.",
                "If no parameters provided - command starts in interactive mode."
            ),
            "-id8=<node-id8>" -> List(
                "ID8 of the node to kill or restart.",
                "Note that either '-id8' or '-id' can be specified and " +
                    "you can also use '@n0' ... '@nn' variables as shortcut to <node-id8>.",
                "If no parameters provided - command starts in interactive mode."
            ),
            "-id=<node-id>" -> List(
                "ID of the node to kill or restart.",
                "Note that either '-id8' or '-id' can be specified.",
                "If no parameters provided - command starts in interactive mode."
            )
        ),
        examples = List(
            cmd.name ->
                "Starts command in interactive mode.",
            s"${cmd.name} -id8=12345678 -r" ->
                "Restart node with id8.",
            s"${cmd.name} -id8=@n0 -r" ->
                "Restart specified node with id8 taken from 'n0' memory variable.",
            s"${cmd.name} -k" ->
                "Kill (stop) all nodes."
        ),
        emptyArgs = cmd.kill,
        withArgs = cmd.kill
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
    implicit def fromKill2Visor(vs: VisorTag): VisorKillCommand = cmd
}
