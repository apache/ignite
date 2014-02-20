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
 * Visor 'cclear' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.cclear.VisorCacheClearCommand._
 * </ex>
 * Note that `VisorCacheClearCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------+
 * | cclear | Clears all entries from cache on all nodes. |
 * +------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor cclear
 *     visor cclear "<cache-name>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, default cache will be cleared.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor cclear
 *         Clears default cache.
 *     visor cclear "cache"
 *         Clears cache with name 'cache'.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object cclear
