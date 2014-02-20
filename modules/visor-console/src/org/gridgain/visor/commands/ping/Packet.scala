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
 * ==Command==
 * Visor 'ping' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.ping.VisorPingCommand._
 * </ex>
 * Note that `VisorPingCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +--------------------+
 * | ping | Pings node. |
 * +--------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor ping {"id81 id82 ... id8k"}
 * }}}
 *
 * ====Arguments====
 * {{{
 *     id8k
 *         ID8 of the node to ping.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor ping "12345678"
 *         Pings node with '12345678' ID8.
 *     visor ping
 *         Pings all nodes in the topology.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object ping
