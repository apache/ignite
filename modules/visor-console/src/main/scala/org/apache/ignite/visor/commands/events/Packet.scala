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
 * Visor 'events' commands implementation.
 *
 * ==Help==
 * {{{
 * +----------------------------------------------------------------------------------------+
 * | events | Print events from a node.                                                     |
 * |        |                                                                               |
 * |        | Note that this command depends on GridGain events.                            |
 * |        |                                                                               |
 * |        | GridGain events can be individually enabled and disabled and disabled events  |
 * |        | can affect the results produced by this command. Note also that configuration |
 * |        | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |        | events on each node can also affect the functionality of this command.        |
 * |        |                                                                               |
 * |        | By default - all events are enabled and GridGain stores last 10,000 local     |
 * |        | events on each node. Both of these defaults can be changed in configuration.  |
 * +----------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     events
 *     events "{-id=<node-id>|-id8=<node-id8>} {-e=<ch,cp,de,di,jo,ta,cl,ca,sw>}
 *         {-t=<num>s|m|h|d} {-s=e|t} {-r} {-c=<n>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full node ID.
 *         Either '-id' or '-id8' can be specified.
 *         If called without the arguments - starts in interactive mode.
 *     -id8
 *         Node ID8.
 *         Either '-id' or '-id8' can be specified.
 *         If called without the arguments - starts in interactive mode.
 *     -e=<ch,de,di,jo,ta,cl,ca,sw>
 *         Comma separated list of event types that should be queried:
 *            ch Checkpoint events.
 *            de Deployment events.
 *            di Discovery events.
 *            jo Job execution events.
 *            ta Task execution events.
 *            ca Cache events.
 *            cp Cache pre-loader events.
 *            sw Swapspace events.
 *     -t=<num>s|m|h|d
 *         Defines time frame for querying events:
 *            =<num>s Queries events fired during last <num> seconds.
 *            =<num>m Queries events fired during last <num> minutes.
 *            =<num>h Queries events fired during last <num> hours.
 *            =<num>d Queries events fired during last <num> days.
 *     -s=e|t
 *         Defines sorting of queried events:
 *            =e Sorted by event type.
 *            =t Sorted chronologically.
 *         Only one '=e' or '=t' can be specified.
 *     -r
 *         Defines if sorting should be reversed.
 *         Can be specified only with -s argument.
 *     -c=<n>
 *         Defines the maximum events count that can be shown.
 *         Values in summary tables are calculated over the whole list of events.
 * }}}
 *
 * ====Examples====
 * {{{
 *     events "-id8=12345678"
 *         Queries all events from node with '12345678' ID8.
 *     events "-id8=12345678 -e=di,ca"
 *         Queries discovery and cache events from node with '12345678' ID8.
 *     events
 *         Starts command in interactive mode.
 * }}}
 */
package object events
