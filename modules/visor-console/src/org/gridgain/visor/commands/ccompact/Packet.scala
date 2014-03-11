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
 * Visor 'ccompact' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.ccompact.VisorCacheCompactCommand._
 * </ex>
 * Note that `VisorCacheCompactCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +--------------------------------------------------------+
 * | ccompact | Compacts all entries in cache on all nodes. |
 * +--------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     ccompact
 *     ccompact "<cache-name>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, entries in default cache will be compacted.
 * }}}
 *
 * ====Examples====
 * {{{
 *     ccompact
 *         Compacts entries in default cache.
 *     ccompact "cache"
 *         Compacts entries in cache with name 'cache'.
 * }}}
 */
package object ccompact
