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

package org.apache.ignite.internal.commandline.dr;

import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.dr.subcommands.DrCacheCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrFullStateTransferCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrHelpCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrNodeCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrPauseCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrResumeCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrStateCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrTopologyCommand;
import org.jetbrains.annotations.NotNull;

/** */
public enum DrSubCommandsList {
    /** */
    HELP("help", new DrHelpCommand()),
    /** */
    STATE("state", new DrStateCommand()),
    /** */
    TOPOLOGY("topology", new DrTopologyCommand()),
    /** */
    NODE("node", new DrNodeCommand()),
    /** */
    CACHE("cache", new DrCacheCommand()),
    /** */
    FULL_STATE_TRANSFER("full-state-transfer", new DrFullStateTransferCommand()),
    /** */
    PAUSE("pause", new DrPauseCommand()),
    /** */
    RESUME("resume", new DrResumeCommand());

    /** */
    private final String name;

    /** */
    private final Command<?> cmd;

    /** */
    DrSubCommandsList(String name, Command<?> cmd) {
        this.name = name;
        this.cmd = cmd;
    }

    /** */
    public String text() {
        return name;
    }

    /** */
    @NotNull
    public Command<?> command() {
        return cmd;
    }

    /** */
    public static DrSubCommandsList parse(String name) {
        for (DrSubCommandsList cmd : values()) {
            if (cmd.name.equalsIgnoreCase(name))
                return cmd;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
