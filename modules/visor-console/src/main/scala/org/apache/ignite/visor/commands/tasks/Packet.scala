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
 * Visor 'tasks' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------------------+
 * | tasks | Prints statistics about tasks and executions.                                 |
 * |       |                                                                               |
 * |       | Note that this command depends on Ignite events.                            |
 * |       |                                                                               |
 * |       | Ignite events can be individually enabled and disabled and disabled events  |
 * |       | can affect the results produced by this command. Note also that configuration |
 * |       | of Event Storage SPI that is responsible for temporary storage of generated   |
 * |       | events on each node can also affect the functionality of this command.        |
 * |       |                                                                               |
 * |       | By default - all events are enabled and Ignite stores last 10,000 local     |
 * |       | events on each node. Both of these defaults can be changed in configuration.  |
 * +---------------------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     tasks
 *     tasks "-l {-t=<num>s|m|h|d} {-r}"
 *     tasks "-s=<substring> {-t=<num>s|m|h|d} {-r}"
 *     tasks "-g {-t=<num>s|m|h|d} {-r}"
 *     tasks "-h {-t=<num>s|m|h|d} {-r}"
 *     tasks "-n=<task-name> {-r}"
 *     tasks "-e=<exec-id>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -l
 *         List all tasks and executions for a given time period.
 *         Executions sorted chronologically (see '-r'), and tasks alphabetically
 *         See '-t=<num>s|m|h|d' on how to specify the time limit.
 *         Default time period is one hour, i.e '-t=1h'
 *
 *         This is a default mode when command is called without parameters.
 *     -s=<substring>
 *         List all tasks and executions for a given task name substring.
 *         Executions sorted chronologically (see '-r'), and tasks alphabetically
 *         See '-t=<num>s|m|h|d' on how to specify the time limit.
 *         Default time period is one hour, i.e '-t=1h'
 *     -g
 *         List all tasks grouped by nodes for a given time period.
 *         Tasks sorted alphabetically
 *         See '-t=<num>s|m|h|d' on how to specify the time limit.
 *         Default time period is one hour, i.e '-t=1h'
 *     -h
 *         List all tasks grouped by hosts for a given time period.
 *         Tasks sorted alphabetically
 *         See '-t=<num>s|m|h|d' on how to specify the time limit.
 *         Default time period is one hour, i.e '-t=1h'
 *     -t=<num>s|m|h|d
 *         Defines time frame for '-l' parameter:
 *            =<num>s Last <num> seconds.
 *            =<num>m Last <num> minutes.
 *            =<num>h Last <num> hours.
 *            =<num>d Last <num> days.
 *     -r
 *         Reverse sorting of executions.
 *     -n=<task-name>
 *         Prints aggregated statistic for named task.
 *     -e=<exec-id>
 *         Prints aggregated statistic for given task execution.
 * }}}
 *
 * ====Examples====
 * {{{
 *     tasks "-l"
 *         Prints list of all tasks and executions for the last hour (default).
 *     tasks
 *         Prints list of all tasks and executions for the last hour (default).
 *     tasks "-l -t=5m"
 *         Prints list of tasks and executions that started during last 5 minutes.
 *     tasks "-s=Task"
 *         Prints list of all tasks and executions that have 'Task' in task name.
 *     tasks "-g"
 *         Prints list of tasks grouped by nodes.
 *     tasks "-g -t=5m"
 *         Prints list of tasks that started during last 5 minutes grouped by nodes.
 *     tasks "-h"
 *         Prints list of tasks grouped by hosts.
 *     tasks "-h -t=5m"
 *         Prints list of tasks that started during last 5 minutes grouped by hosts.
 *     tasks "-n=GridTask"
 *         Prints summary for task named 'GridTask'.
 *     tasks "-e=7D5CB773-225C-4165-8162-3BB67337894B"
 *         Traces task execution with ID '7D5CB773-225C-4165-8162-3BB67337894B'.
 *     tasks "-e=@e1"
 *         Traces task execution with ID taken from 'e1' memory variable.
 * }}}
 */
package object tasks
