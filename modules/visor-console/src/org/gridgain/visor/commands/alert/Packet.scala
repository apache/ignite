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
 * Visor 'alert' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to
 * properly import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.alert.VisorAlertCommand._
 * </ex>
 * Note that `VisorAlertCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------+
 * | alert | Generates email alerts for user-defined events.             |
 * |       | Node events and grid-wide events are defined via mnemonics. |
 * +---------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor alert
 *     visor alert "-u {-id=<alert-id>|-a}"
 *     visor alert "-r {-t=<sec>} -c1=e1<num> -c2=e2<num> ... -ck=ek<num>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -u
 *         Unregisters alert(s). Either '-a' flag or '-id' parameter is required.
 *         Note that only one of the '-u' or '-r' is allowed.
 *         If neither '-u' or '-r' provided - all alerts will be printed.
 *     -a
 *         When provided with '-u' - all alerts will be unregistered.
 *     -id=<alert-id>
 *         When provided with '-u' - alert with matching ID will be unregistered.
 *     -r
 *         Register new alert with mnemonic predicate(s).
 *         Note that only one of the '-u' or '-r' is allowed.
 *         If neither '-u' or '-r' provided - all alerts will be printed.
 *     -t
 *         Defines notification frequency in seconds. Default is 15 minutes.
 *         This parameter can only appear with '-r'.
 *     -ck=ek<num>
 *         This defines a mnemonic for the metric that will be measured:
 *         Grid-wide metrics (not node specific):
 *            -cc Total number of available CPUs in the grid.
 *            -nc Total number of nodes in the grid.
 *            -hc Total number of physical hosts in the grid.
 *            -cl Current average CPU load (in %) in the grid.
 *
 *         Per-node current metrics:
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
 *            -cd Current CPU load on the node.
 *            -hm Heap memory maximum (in MB) on the node.
 *
 *         Comparison part of the mnemonic predicate:
 *            =eq<num> Equal '=' to '<num>' number.
 *            =neq<num> Not equal '!=' to '<num>' number.
 *            =gt<num> Greater than '>' to '<num>' number.
 *            =gte<num> Greater than or equal '>=' to '<num>' number.
 *            =lt<num> Less than '<' to 'NN' number.
 *            =lte<num> Less than or equal '<=' to '<num>' number.
 *
 *         NOTE: Email notification will be sent for the alert only when all
 *               provided mnemonic predicates evaluate to 'true'.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor alert
 *         Prints all currently registered alerts.
 *     visor alert "-u -a"
 *         Unregisters all currently registered alerts.
 *     visor alert "-u -id=12345678"
 *         Unregisters alert with provided ID.
 *     visor alert "-r -t=900 -cc=gte4 -cl=gt50"
 *         Notify every 15 min if grid has >= 4 CPUs and > 50% CPU load.
 * }}}
 */
package object alert
