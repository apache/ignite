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

package org.apache.ignite.internal.commandline.snapshot;

import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCheckTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTask;
import org.jetbrains.annotations.Nullable;

/**
 * Set of snapshot sub-commands.
 *
 * @see SnapshotCommand
 */
public enum SnapshotSubcommand {
    /** Sub-command to create a cluster snapshot. */
    CREATE(new SnapshotBasicSubCommand("create", "Create cluster snapshot", VisorSnapshotCreateTask.class)),

    /** Sub-command to cancel running snapshot. */
    CANCEL(new SnapshotBasicSubCommand("cancel", "Cancel running snapshot", VisorSnapshotCancelTask.class)),

    /** Sub-command to check snapshot. */
    CHECK(new SnapshotBasicSubCommand("check", "Check snapshot", VisorSnapshotCheckTask.class)),

    /** Sub-command to restore cache group from snapshot. */
    RESTORE(new SnapshotRestoreSubCommand("restore"));

    /** Command. */
    private final Command<?> cmd;

    /**
     * @param cmd Command.
     */
    SnapshotSubcommand(Command<?> cmd) {
        this.cmd = cmd;
    }

    /**
     * @param text Command text (case insensitive).
     * @return Command for the text. {@code Null} if there is no such command.
     */
     @Nullable public static SnapshotSubcommand of(String text) {
        for (SnapshotSubcommand cmd : values()) {
            if (cmd.subcommand().name().equalsIgnoreCase(text))
                return cmd;
        }

        throw new IllegalArgumentException("Expected correct action: " + text);
    }

    /** @return sub-command. */
    public Command<?> subcommand() {
        return cmd;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return subcommand().name();
    }
}
