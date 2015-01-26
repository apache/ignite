/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.visor.commands

/**
 * ==Overview==
 * Contains Visor command `top` implementation.
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
 *     top "{-c1=e1<num> -c2=e2<num> ... -ck=ek<num>} {-h=<host1> ... -h=<hostk>} {-a}"
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
 *     top "-cc=eq2"
 *         Prints topology for all nodes with two CPUs.
 *     top "-cc=eq2 -a"
 *         Prints full information for all nodes with two CPUs.
 *     top "-h=10.34.2.122 -h=10.65.3.11"
 *         Prints topology for provided hosts.
 *     top
 *         Prints full topology.
 * }}}
 */
package object top
