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

import org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsOperation;
import org.jetbrains.annotations.Nullable;

/**
 * Set of performance statistics sub-commands.
 *
 * @see PerformanceStatisticsCommand
 */
public enum PerformanceStatisticsSubCommand {
    /** Sub-command to start collecting performance statistics in the cluster. */
    START("start", VisorPerformanceStatisticsOperation.START),

    /** Sub-command to stop collecting performance statistics in the cluster. */
    STOP("stop", VisorPerformanceStatisticsOperation.STOP),

    /** Sub-command to rotate collecting performance statistics in the cluster. */
    ROTATE("rotate", VisorPerformanceStatisticsOperation.ROTATE),

    /** Sub-command to get status of collecting performance statistics in the cluster. */
    STATUS("status", VisorPerformanceStatisticsOperation.STATUS);

    /** Sub-command name. */
    private final String name;

    /** Corresponding visor performance statistics operation. */
    private final VisorPerformanceStatisticsOperation visorOp;

    /**
     * @param name Performance statistics sub-command name.
     * @param visorOp Corresponding visor performance statistics operation.
     */
    PerformanceStatisticsSubCommand(String name, VisorPerformanceStatisticsOperation visorOp) {
        this.name = name;
        this.visorOp = visorOp;
    }

    /**
     * @param text Command text (case insensitive).
     * @return Command for the text. {@code Null} if there is no such command.
     */
    @Nullable public static PerformanceStatisticsSubCommand of(String text) {
        for (PerformanceStatisticsSubCommand cmd : values()) {
            if (cmd.name.equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return {@link VisorPerformanceStatisticsOperation} which is associated with performance statistics subcommand.
     */
    public VisorPerformanceStatisticsOperation visorOperation() {
        return visorOp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
