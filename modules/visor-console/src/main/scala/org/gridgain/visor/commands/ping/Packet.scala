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
 * ==Command==
 * Visor 'ping' command implementation.
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
 *     ping {"id81 id82 ... id8k"}
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
 *     ping "12345678"
 *         Pings node with '12345678' ID8.
 *     ping
 *         Pings all nodes in the topology.
 * }}}
 */
package object ping
