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

package org.apache.ignite.internal.commandline.persistence.cleaning;

import org.apache.ignite.internal.visor.persistence.cleaning.PersistenceCleaningOperation;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public enum PersistenceCleaningSubcommands {
    /** Collects information about corrupted caches and cache groups and their file system paths. */
    INFO("info", PersistenceCleaningOperation.INFO),

    /** Cleans partition files of corrupted caches and cache groups. */
    CLEAN("clean", PersistenceCleaningOperation.CLEAN);

    /** Subcommand name. */
    private final String name;

    /** Operation this subcommand triggers. */
    private final PersistenceCleaningOperation operation;

    /**
     * @param name String representation of subcommand.
     * @param operation Operation this command triggers.
     */
    PersistenceCleaningSubcommands(String name, PersistenceCleaningOperation operation) {
        this.name = name;
        this.operation = operation;
    }

    /**
     * @param strRep String representation of subcommand.
     * @return Subcommand for its string representation.
     */
    public static @Nullable PersistenceCleaningSubcommands of(String strRep) {
        for (PersistenceCleaningSubcommands cmd : values()) {
            if (cmd.name().equals(strRep))
                return cmd;
        }

        return null;
    }

    /** */
    public PersistenceCleaningOperation cleaningOperation() {
        return operation;
    }
}
