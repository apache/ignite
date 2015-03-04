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
 * Visor 'deploy' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------+
 * | deploy | Copies file or directory to remote host. |
 * |        | Command relies on SFTP protocol.         |
 * +---------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     deploy "-h={<username>{:<password>}@}<host>{:<port>} {-u=<username>}
 *         {-p=<password>} {-k=<path>} -s=<path> {-d<path>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -h={<username>{:<password>}@}<host>{:<port>}
 *         Host specification.
 *
 *         <host> can be a hostname, IP or range of IPs.
 *         Example of range is 192.168.1.100~150,
 *         which means all IPs from 192.168.1.100 to 192.168.1.150 inclusively.
 *
 *         Default port number is 22.
 *
 *         This option can be provided multiple times.
 *     -u=<username>
 *         Default username.
 *         Used if specification doesn't contain username.
 *         If default is not provided as well, current local username will be used.
 *     -p=<password>
 *         Default password.
 *         Used if specification doesn't contain password.
 *         If default is not provided as well, it will be asked interactively.
 *     -k=<path>
 *         Path to private key file.
 *         If provided, it will be used for all specifications that doesn't contain password.
 *     -s=<path>
 *         Source path.
 *     -d=<path>
 *         Destination path (relative to IGNITE_HOME).
 *         If not provided, files will be copied to the root of IGNITE_HOME.
 * }}}
 *
 * ====Examples====
 * {{{
 *     deploy "-h=uname:passwd@host -s=/local/path -d=/remote/path"
 *         Copies file or directory to remote host (password authentication).
 *     deploy "-h=uname@host -k=ssh-key.pem -s=/local/path -d=/remote/path"
 *         Copies file or directory to remote host (private key authentication).
 * }}}
 */
package object deploy
