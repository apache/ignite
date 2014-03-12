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
 * Visor 'cscan' command implementation.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------+
 * | cscan | List all entries from cache on all nodes.    |
 * +------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cscan {-id=<node-id>|-id8=<node-id8>} {-p=<page size>} -c=<cache name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <node-id>
 *         Full node ID.
 *     <node-id8>
 *         Node ID8.
 *     <page size>
 *         Number of object to fetch from cache at once.
 *     <cache-name>
 *         Name of the cache.
 * }}}
 *
 * ====Examples====
 * {{{
 *    cscan -c=cache
 *        List entries from cache with name 'cache' from all nodes with this cache.
 *    cscan -p=50 -c=@c0
 *        List entries from cache with name taken from 'c0' memory variable with page of 50 items from all nodes with this cache.
 *    cscan -id8=12345678 -c=cache
 *        List entries from cache with name 'cache' and node '12345678' ID8.
 * }}}
 */
package object cscan
