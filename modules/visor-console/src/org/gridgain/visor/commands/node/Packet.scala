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
 *     visor node "{id8=<node-id8>|id=<node-id>} {-a}"
 *     visor node
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
 *     visor node
 *         Starts command in interactive mode.
 *     visor node "-id8=12345678"
 *         Prints statistics for specified node.
 *     visor node "-id8=12345678 -a"
 *         Prints full statistics for specified node.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object node
