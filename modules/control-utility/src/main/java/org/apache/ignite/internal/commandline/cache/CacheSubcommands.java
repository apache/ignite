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

package org.apache.ignite.internal.commandline.cache;

import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.DeclarativeCommandAdapter;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.IndexRebuildCommandArg;
import org.apache.ignite.internal.management.cache.CacheCheckIndexInlineSizesCommand;
import org.apache.ignite.internal.management.cache.CacheClearCommand;
import org.apache.ignite.internal.management.cache.CacheContentionCommand;
import org.apache.ignite.internal.management.cache.CacheCreateCommand;
import org.apache.ignite.internal.management.cache.CacheDestroyCommand;
import org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand;
import org.apache.ignite.internal.management.cache.CacheIndexesListCommand;
import org.apache.ignite.internal.management.cache.CacheIndexesRebuildStatusCommand;
import org.apache.ignite.internal.management.cache.CacheListCommand;
import org.apache.ignite.internal.management.cache.CacheMetricsCommand;
import org.apache.ignite.internal.management.cache.CacheResetLostPartitionsCommand;
import org.apache.ignite.internal.management.cache.CacheScheduleIndexesRebuildCommand;
import org.apache.ignite.internal.management.cache.CacheValidateIndexesCommand;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;

/**
 *
 */
public enum CacheSubcommands {
    /**
     * Prints out help for the cache command.
     */
    HELP("help", null, null),

    /**
     * Checks consistency of primary and backup partitions assuming no concurrent updates are happening in the cluster.
     */
    IDLE_VERIFY("idle_verify", IdleVerifyCommandArg.class, new IdleVerify()),

    /**
     * Prints info regarding caches, groups or sequences.
     */
    LIST(new CacheListCommand()),

    /**
     * Create caches.
     */
    CREATE(new CacheCreateCommand()),

    /**
     * Destroy caches.
     */
    DESTROY(new CacheDestroyCommand()),

    /**
     * Clear caches.
     */
    CLEAR(new CacheClearCommand()),

    /**
     * Validates indexes attempting to read each indexed entry.
     */
    VALIDATE_INDEXES(new CacheValidateIndexesCommand()),

    /**
     * Check secondary indexes inline size.
     */
    CHECK_INDEX_INLINE_SIZES(new CacheCheckIndexInlineSizesCommand()),

    /**
     * Prints info about contended keys (the keys concurrently locked from multiple transactions).
     */
    CONTENTION(new CacheContentionCommand()),

    /**
     * Collect information on the distribution of partitions.
     */
    DISTRIBUTION("distribution", DistributionCommandArg.class, new CacheDistribution()),

    /**
     * Reset lost partitions
     */
    RESET_LOST_PARTITIONS(new CacheResetLostPartitionsCommand()),

    /**
     * Find and remove garbage.
     */
    FIND_AND_DELETE_GARBAGE("find_garbage", FindAndDeleteGarbageArg.class, new FindAndDeleteGarbage()),

    /**
     * Index list.
     */
    INDEX_LIST(new CacheIndexesListCommand()),

    /**
     * Index rebuild status.
     */
    INDEX_REBUILD_STATUS(new CacheIndexesRebuildStatusCommand()),

    /**
     * Index force rebuild.
     */
    INDEX_FORCE_REBUILD(new CacheIndexesForceRebuildCommand()),

    /**
     * Enable / disable cache metrics collection or show metrics collection status.
     */
    METRICS(new CacheMetricsCommand()),

    /**
     * Index rebuild via the maintenance mode.
     */
    INDEX_REBUILD(new CacheScheduleIndexesRebuildCommand()),

    /**
     * Scan cache entries.
     */
    SCAN("scan", IndexRebuildCommandArg.class, new CacheScan());

    /** Enumerated values. */
    private static final CacheSubcommands[] VALS = values();

    /** Enum class with argument list for command. */
    private final Class<? extends Enum<? extends CommandArg>> commandArgs;

    /** Name. */
    private final String name;

    /** Command instance for certain type. */
    private final Command command;

    /**
     * @param name Name.
     * @param command Command realization.
     */
    CacheSubcommands(
        String name,
        Class<? extends Enum<? extends CommandArg>> commandArgs,
        Command command
    ) {
        this.name = name;
        this.commandArgs = commandArgs;
        this.command = command;
    }

    /** @param command Management API command. */
    CacheSubcommands(org.apache.ignite.internal.management.api.Command<?, ?> command) {
        this.name = toFormattedCommandName(command.getClass(), PARAM_WORDS_DELIM).substring("cache_".length());
        this.command = new DeclarativeCommandAdapter<>(command);
        this.commandArgs = null;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static CacheSubcommands of(String text) {
        for (CacheSubcommands cmd : CacheSubcommands.values()) {
            if (cmd.text().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return Name.
     */
    public String text() {
        return name;
    }

    /**
     * @return Subcommand realization.
     */
    public Command subcommand() {
        return command;
    }

    /**
     * @return Enum class with argument list for command.
     */
    public Class<? extends Enum<? extends CommandArg>> getCommandArgs() {
        return commandArgs;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheSubcommands fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
