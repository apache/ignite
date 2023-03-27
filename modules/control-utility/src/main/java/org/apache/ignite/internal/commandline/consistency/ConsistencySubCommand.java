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

package org.apache.ignite.internal.commandline.consistency;

import org.apache.ignite.internal.visor.consistency.VisorConsistencyCountersFinalizationTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyStatusTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public enum ConsistencySubCommand {
    /** Repair. */
    REPAIR("repair", VisorConsistencyRepairTask.class.getName()),

    /** Status. */
    STATUS("status", VisorConsistencyStatusTask.class.getName()),

    /** Finalize partitions update counters. */
    FINALIZE_COUNTERS("finalize", VisorConsistencyCountersFinalizationTask.class.getName());

    /** Sub-command name. */
    private final String name;

    /** Task class name to execute. */
    private final String taskName;

    /**
     * @param name Name.
     * @param taskName Task name.
     */
    ConsistencySubCommand(String name, String taskName) {
        this.name = name;
        this.taskName = taskName;
    }

    /**
     * @param text Command text (case insensitive).
     * @return Command for the text. {@code Null} if there is no such command.
     */
    @Nullable public static ConsistencySubCommand of(String text) {
        for (ConsistencySubCommand cmd : values()) {
            if (cmd.name.equalsIgnoreCase(text))
                return cmd;
        }

        throw new IllegalArgumentException("Expected correct action: " + text);
    }

    /**
     * @return Task class name to execute.
     */
    public String taskName() {
        return taskName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
