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
 * Visor 'cclear' command implementation.
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
 *     cclear
 *     cclear "<cache-name>"
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
 *     cclear
 *         Clears default cache.
 *     cclear "cache"
 *         Clears cache with name 'cache'.
 * }}}
 */
package object cclear
