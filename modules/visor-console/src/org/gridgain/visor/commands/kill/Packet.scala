// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands

/**
 * ==Overview==
 * Contains Visor command `kill` implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.kill.VisorKillCommand._
 * </ex>
 * Note that `VisorKillCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
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
 *     visor kill
 *     visor kill "-in|-ih"
 *     visor kill "{-r|-k} {-id8=<node-id8>|-id=<node-id>}"
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
 *     visor kill
 *         Starts command in interactive mode.
 *     visor kill "-id8=12345678 -r"
 *         Restart node with '12345678' ID8.
 *     visor kill "-k"
 *         Kill (stop) all nodes.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object kill
