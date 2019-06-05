/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.diagnostic;

import org.apache.ignite.internal.commandline.Command;

/**
 *
 */
public enum DiagnosticSubCommand {
    /** */
    HELP("help", null),

    /** */
    PAGE_LOCKS("pageLocks", new PageLocksCommand());

    /** Diagnostic command name. */
    private final String name;

    /** Command instance for certain type. */
    private final Command command;

    /**
     * @param name Command name.
     * @param command Command handler.
     */
    DiagnosticSubCommand(
        String name,
        Command command
    ) {
        this.name = name;
        this.command = command;
    }

    /**
     * @return Subcommand realization.
     */
    public Command subcommand() {
        return command;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static DiagnosticSubCommand of(String text) {
        for (DiagnosticSubCommand cmd : DiagnosticSubCommand.values()) {
            if (cmd.name.equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
