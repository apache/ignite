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
 * Visor 'config' command implementation.
 *
 * ==Help==
 * {{{
 * +-------------------------------------+
 * | config | Prints node configuration. |
 * +-------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     config
 *     config "{-id=<node-id>|id8=<node-id8>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -id=<node-id>
 *         Full node ID. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 *     -id8=<node-id8>
 *         Node ID8. Either '-id8' or '-id' can be specified.
 *         If neither is specified - command starts in interactive mode.
 * }}}
 *
 * ====Examples====
 * {{{
 *     config "-id8=12345678"
 *         Prints configuration for node with '12345678' ID8.
 *     config
 *         Starts command in interactive mode.
 * }}}
 */
package object config
