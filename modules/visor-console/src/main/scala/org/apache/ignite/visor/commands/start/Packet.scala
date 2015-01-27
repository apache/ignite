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
 * Contains Visor command `start` implementation.
 *
 * ==Help==
 * {{{
 * +-----------------------------------------------------+
 * | start | Starts one or more nodes on remote host(s). |
 * |       | Uses SSH protocol to execute commands.      |
 * +-----------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     start "-f=<path> {-m=<num>} {-r}"
 *     start "-h=<hostname> {-p=<num>} {-u=<username>} {-pw=<password>} {-k=<path>}
 *         {-n=<num>} {-g=<path>} {-c=<path>} {-s=<path>} {-m=<num>} {-r}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -f=<path>
 *         Path to INI file that contains topology specification.
 *     -h=<hostname>
 *         Hostname where to start nodes.
 *
 *         Can define several hosts if their IPs are sequential.
 *         Example of range is 192.168.1.100~150,
 *         which means all IPs from 192.168.1.100 to 192.168.1.150 inclusively.
 *     -p=<num>
 *         Port number (default is 22).
 *     -u=<username>
 *         Username (if not defined, current local username will be used).
 *     -pw=<password>
 *         Password (if not defined, private key file must be defined).
 *     -k=<path>
 *         Path to private key file. Define if key authentication is used.
 *     -n=<num>
 *         Expected number of nodes on the host.
 *         If some nodes are started already, then only remaining nodes will be started.
 *         If current count of nodes is equal to this number and '-r' flag is not set, then nothing will happen.
 *     -g=<path>
 *         Path to GridGain installation folder.
 *         If not defined, IGNITE_HOME environment variable must be set on remote hosts.
 *     -c=<path>
 *         Path to configuration file (relative to GridGain home).
 *         If not provided, default GridGain configuration is used.
 *     -s=<path>
 *         Path to start script (relative to GridGain home).
 *         Default is "bin/ggstart.sh" for Unix or
 *         "bin\ggstart.bat" for Windows.
 *     -m=<num>
 *         Defines maximum number of nodes that can be started in parallel on one host.
 *         This actually means number of parallel SSH connections to each SSH server.
 *         Default is 5.
 *     -r
 *         Indicates that existing nodes on the host will be restarted.
 *         By default, if flag is not present, existing nodes will be left as is.
 * }}}
 *
 * ====Examples====
 * {{{
 *     start "-h=10.1.1.10 -u=uname -pw=passwd -n=3"
 *         Starts three nodes with default configuration (password authentication).
 *     start "-h=192.168.1.100~104 -u=uname -k=/home/uname/.ssh/is_rsa -n=5"
 *         Starts 25 nodes on 5 hosts (5 nodes per host) with default configuration (key-based authentication).
 *     start "-f=start-nodes.ini -r"
 *         Starts topology defined in 'start-nodes.ini' file. Existing nodes are stopped.
 * }}}
 */
package object start
