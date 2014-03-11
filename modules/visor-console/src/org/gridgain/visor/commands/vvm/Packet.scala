/* @scala.file.header */

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
 * Contains Visor command `vvm` implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.vvm.VisorVvmCommand._
 * </ex>
 * Note that `VisorVvmCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +-----------------------+
 * | vvm | Opens VisualVM. |
 * +-----------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     vvm "{-home=dir} {-id8=<node-id8>} {-id=<node-id>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -home=dir
 *         VisualVM home directory.
 *         If not specified, PATH and JAVA_HOME will be searched
 *     -id8=<node-id8>
 *         ID8 of node.
 *         Either '-id8' or '-id' can be specified.
 *     -id=<node-id>
 *         Full ID of node.
 *         Either '-id8' or '-id' can be specified.
 * }}}
 *
 * ====Examples====
 * {{{
 *     vvm "-id8=12345678"
 *         Opens VisualVM connected to JVM for node with '12345678' ID8.
 *     vvm "-id=5B923966-85ED-4C90-A14C-96068470E94D"
 *         Opens VisualVM connected to JVM for node with given full node ID.
 *     vvm "-home=C:\VisualVM -id8=12345678"
 *         Opens VisualVM installed in 'C:\VisualVM' directory for specified node.
 *     vvm
 *         Opens VisualVM connected to all nodes.
 * }}}
 */
package object vvm
