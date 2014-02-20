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
 * Contains Visor command `gc` implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.gc.VisorGcCommand._
 * </ex>
 * Note that `VisorGcCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------+
 * | gc | Runs garbage collector on remote nodes.                              |
 * |    | If specific node is provided, garbage collector is run on that node. |
 * |    | Otherwise, it will be run on all nodes in topology.                  |
 * +---------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor gc
 *     visor gc "{-id8=<node-id8>|-id=<node-id>} {-c}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id8=<node-id8>
 *         ID8 of the node.
 *         Note that either '-id8' or '-id' can be specified.
 *     -id=<node-id>
 *         ID of the node.
 *         Note that either '-id8' or '-id' can be specified.
 *     -c
 *         Run DGC procedure on all caches.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor gc "-id8=12345678"
 *         Runs garbage collector on specified node.
 *     visor gc
 *         Runs garbage collector on all nodes in topology.
 *     visor gc "-id8=12345678 -c"
 *         Runs garbage collector and DGC procedure on all caches.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object gc
