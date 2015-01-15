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
 * Contains Visor command `vvm` implementation.
 *
 * ==Help==
 * {{{
 * +-----------------------+
 * | vvm | Opens VisualVM. |
 * +-----------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     vvm "{-home=dir} {-id8=<node-id8>} {-id=<node-id>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -home=dir
 *         VisualVM home directory.
 *         If not specified, PATH and JAVA_HOME will be searched
 *     -id8=<node-id8>
 *         ID8 of node.
 *         Either '-id8' or '-id' can be specified.
 *     -id=<node-id>
 *         Full ID of node.
 *         Either '-id8' or '-id' can be specified.
 * }}}
 *
 * ====Examples====
 * {{{
 *     vvm "-id8=12345678"
 *         Opens VisualVM connected to JVM for node with '12345678' ID8.
 *     vvm "-id=5B923966-85ED-4C90-A14C-96068470E94D"
 *         Opens VisualVM connected to JVM for node with given full node ID.
 *     vvm "-home=C:\VisualVM -id8=12345678"
 *         Opens VisualVM installed in 'C:\VisualVM' directory for specified node.
 *     vvm
 *         Opens VisualVM connected to all nodes.
 * }}}
 */
package object vvm
