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

package org.apache.ignite.internal.commandline.encryption;

import org.apache.ignite.internal.commandline.Command;
import org.jetbrains.annotations.Nullable;

/**
 * Set of encryption subcommands.
 *
 * @see EncryptionCommands
 */
public enum EncryptionSubcommands {
    /** Subcommand to get the current master key name. */
    GET_MASTER_KEY_NAME("get_master_key_name", new GetMasterKeyNameCommand()),

    /** Subcommand to change the master key. */
    CHANGE_MASTER_KEY("change_master_key", new ChangeMasterKeyCommand()),

    /** Subcommand to change the current encryption key for specified cache group. */
    CHANGE_CACHE_GROUP_KEY("change_cache_key", new ChangeCacheGroupKeyCommand()),

    /** Subcommand to view current encryption key IDs for specified cache group. */
    CACHE_GROUP_KEY_IDS("cache_key_ids", new CacheGroupKeysCommand()),

    /** Subcommand to view re-encryption status of cache group. */
    REENCRYPTION_STATUS("reencryption_status", new EncryptionStatusCommand()),

    /** Subcommand to stop cache group reencryption. */
    STOP_REENCRYPTION("stop_reencryption", new StopReencryptionCommand()),

    /** Subcommand to start cache group reencryption. */
    START_REENCRYPTION("start_reencryption", new StartReencryptionCommand()),

    /** Subcommand to view/change cache group reencryption rate. */
    REENCRYPTION_RATE("reencryption_rate", new ReencryptionRateCommand());

    /** Subcommand name. */
    private final String name;

    /** Command. */
    private final Command command;

    /**
     * @param name Encryption subcommand name.
     * @param command Command implementation.
     */
    EncryptionSubcommands(String name, Command command) {
        this.name = name;
        this.command = command;
    }

    /**
     * @return Cache subcommand implementation.
     */
    public Command subcommand() {
        return command;
    }

    /**
     * @param text Command text (case insensitive).
     * @return Command for the text. {@code Null} if there is no such command.
     */
     @Nullable public static EncryptionSubcommands of(String text) {
        for (EncryptionSubcommands cmd : values()) {
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
