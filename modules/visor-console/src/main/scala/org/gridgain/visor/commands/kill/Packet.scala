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

package org.gridgain.visor.commands

/**
 * ==Overview==
 * Contains Visor command `kill` implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------+
 * | kill | Kills or restarts node. |
 * +--------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     kill
 *     kill "-in|-ih"
 *     kill "{-r|-k} {-id8=<node-id8>|-id=<node-id>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -in
 *         Run command in interactive mode with ability to
 *         choose a node to kill or restart.
 *         Note that either '-in' or '-ih' can be specified.
 *
 *         This mode is used by default.
 *     -ih
 *         Run command in interactive mode with ability to
 *         choose a host where to kill or restart nodes.
 *         Note that either '-in' or '-ih' can be specified.
 *     -r
 *         Restart node mode.
 *         Note that either '-r' or '-k' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -k
 *         Kill (stop) node mode.
 *         Note that either '-r' or '-k' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -id8=<node-id8>
 *         ID8 of the node to kill or restart.
 *         Note that either '-id8' or '-id' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 *     -id=<node-id>
 *         ID of the node to kill or restart.
 *         Note that either '-id8' or '-id' can be specified.
 *         If no parameters provided - command starts in interactive mode.
 * }}}
 *
 * ====Examples====
 * {{{
 *     kill
 *         Starts command in interactive mode.
 *     kill "-id8=12345678 -r"
 *         Restart node with '12345678' ID8.
 *     kill "-k"
 *         Kill (stop) all nodes.
 * }}}
 */
package object kill
