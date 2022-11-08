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

package org.apache.ignite.internal.commandline.performancestatistics;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTask;
import org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTaskArg;
import org.apache.ignite.mxbean.PerformanceStatisticsMBean;

import static org.apache.ignite.internal.commandline.CommandList.PERFORMANCE_STATISTICS;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.ROTATE;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.START;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.STATUS;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.STOP;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.of;

/**
 * Performance statistics command.
 *
 * @see PerformanceStatisticsProcessor
 * @see PerformanceStatisticsMBean
 */
public class PerformanceStatisticsCommand extends AbstractCommand<Object> {
    /** Command argument. */
    private VisorPerformanceStatisticsTaskArg taskArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            String res = executeTaskByNameOnNode(
                client,
                VisorPerformanceStatisticsTask.class.getName(),
                taskArgs,
                null,
                clientCfg
            );

            log.info(res);

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        PerformanceStatisticsSubCommand cmd = of(argIter.nextArg("Expected performance statistics action."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct performance statistics action.");

        taskArgs = new VisorPerformanceStatisticsTaskArg(cmd.visorOperation());
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        usage(log, "Start collecting performance statistics in the cluster:",
            PERFORMANCE_STATISTICS, START.toString());

        usage(log, "Stop collecting performance statistics in the cluster:",
            PERFORMANCE_STATISTICS, STOP.toString());

        usage(log, "Rotate collecting performance statistics in the cluster:",
            PERFORMANCE_STATISTICS, ROTATE.toString());

        usage(log, "Get status of collecting performance statistics in the cluster:",
            PERFORMANCE_STATISTICS, STATUS.toString());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PERFORMANCE_STATISTICS.toCommandName();
    }
}
