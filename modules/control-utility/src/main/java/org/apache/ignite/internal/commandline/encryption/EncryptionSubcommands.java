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

    /** Subcommand to view current encryption key IDs of the cache group. */
    CACHE_GROUP_KEY_IDS("cache_key_ids", new CacheGroupEncryptionCommand.CacheKeyIds()),

    /** Subcommand to display re-encryption status of the cache group. */
    REENCRYPTION_STATUS("reencryption_status", new CacheGroupEncryptionCommand.ReencryptionStatus()),

    /** Subcommand to suspend re-encryption of the cache group. */
    REENCRYPTION_SUSPEND("suspend_reencryption", new CacheGroupEncryptionCommand.SuspendReencryption()),

    /** Subcommand to resume re-encryption of the cache group. */
    REENCRYPTION_RESUME("resume_reencryption", new CacheGroupEncryptionCommand.ResumeReencryption()),

    /** Subcommand to view/change cache group re-encryption rate limit. */
    REENCRYPTION_RATE("reencryption_rate_limit", new ReencryptionRateCommand());

    /** Subcommand name. */
    private final String name;

    /** Command. */
    private final Command<?> cmd;

    /**
     * @param name Encryption subcommand name.
     * @param cmd Command implementation.
     */
    EncryptionSubcommands(String name, Command<?> cmd) {
        this.name = name;
        this.cmd = cmd;
    }

    /**
     * @return Name.
     */
    public String text() {
        return name;
    }

    /**
     * @return Cache subcommand implementation.
     */
    public Command<?> subcommand() {
        return cmd;
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
