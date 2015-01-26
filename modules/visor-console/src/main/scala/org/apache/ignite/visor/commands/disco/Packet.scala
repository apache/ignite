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
 * Visor 'disco' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------------------+
 * | disco | Prints topology change log as seen from the oldest node.                      |
 * |       | Timeframe for quering events can be specified in arguments.                   |
 * |       |                                                                               |
 * |       | Note that this command depends on GridGain events.                            |
 * |       |                                                                               |
 * |       | GridGain events can be individually enabled and disabled and disabled events  |
 * |       | can affect the results produced by this command. Note also that configuration |
 * |       | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |       | events on each node can also affect the functionality of this command.        |
 * |       |                                                                               |
 * |       | By default - all events are enabled and GridGain stores last 10,000 local     |
 * |       | events on each node. Both of these defaults can be changed in configuration.  |
 * +---------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     disco
 *     disco "{-t=<num>s|m|h|d} {-r} {-c=<n>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -t=<num>s|m|h|d
 *         Defines timeframe for querying events:
 *            =<num>s Events fired during last <num> seconds.
 *            =<num>m Events fired during last <num> minutes.
 *            =<num>h Events fired during last <num> hours.
 *            =<num>d Events fired during last <num> days.
 *     -r
 *         Defines whether sorting should be reversed.
 *     -c=<n>
 *         Defines the maximum events count that can be shown.
 * }}}
 *
 * ====Examples====
 * {{{
 *     disco
 *         Prints all discovery events sorted chronologically (oldest first).
 *     disco "-r"
 *         Prints all discovery events sorted chronologically in reversed order (newest first).
 *     disco "-t=2m"
 *         Prints discovery events fired during last two minutes sorted chronologically.
 * }}}
 */
package object disco
