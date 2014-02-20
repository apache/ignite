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
 * Contains Visor command `top` implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.top.VisorTopologyCommand._
 * </ex>
 * Note that `VisorTopologyCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | top | Prints current topology. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor top "{-c1=e1<num> -c2=e2<num> ... -ck=ek<num>} {-h=<host1> ... -h=<hostk>} {-a}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -ck=ek<num>
 *         This defines a mnemonic for node filter:
 *            -cc Number of available CPUs on the node.
 *            -cl Average CPU load (in %) on the node.
 *            -aj Active jobs on the node.
 *            -cj Cancelled jobs on the node.
 *            -tc Thread count on the node.
 *            -it Idle time on the node.
 *                Note: <num> can have 's', 'm', or 'h' suffix indicating
 *                seconds, minutes, and hours. By default (no suffix provided)
 *                value is assumed to be in milliseconds.
 *            -ut Up time on the node.
 *                Note: <num> can have 's', 'm', or 'h' suffix indicating
 *                seconds, minutes, and hours. By default (no suffix provided)
 *                value is assumed to be in milliseconds.
 *            -je Job execute time on the node.
 *            -jw Job wait time on the node.
 *            -wj Waiting jobs count on the node.
 *            -rj Rejected jobs count on the node.
 *            -hu Heap memory used (in MB) on the node.
 *            -hm Heap memory maximum (in MB) on the node.
 *
 *         Comparison part of the mnemonic predicate:
 *            =eq<num> Equal '=' to '<num>' number.
 *            =neq<num> Not equal '!=' to '<num>' number.
 *            =gt<num> Greater than '>' to '<num>' number.
 *            =gte<num> Greater than or equal '>=' to '<num>' number.
 *            =lt<num> Less than '<' to '<num>' number.
 *            =lte<num> Less than or equal '<=' to '<num>' number.
 *     -h=<host>
 *         This defines a host to show nodes from.
 *         Multiple hosts can be provided.
 *     -a
 *         This defines whether to show a separate table of nodes
 *         with detail per-node information.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor top "-cc=eq2"
 *         Prints topology for all nodes with two CPUs.
 *     visor top "-cc=eq2 -a"
 *         Prints full information for all nodes with two CPUs.
 *     visor top "-h=10.34.2.122 -h=10.65.3.11"
 *         Prints topology for provided hosts.
 *     visor top
 *         Prints full topology.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
package object top
