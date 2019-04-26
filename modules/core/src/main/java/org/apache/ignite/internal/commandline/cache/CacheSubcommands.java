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
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;

/**
 *
 */
public enum CacheSubcommands {
    /**
     * Prints out help for the cache command.
     */
    HELP("help", null, "Print how to use other cache commands.", null),

    /**
     * Checks consistency of primary and backup partitions assuming no concurrent updates are happening in the cluster.
     */
    IDLE_VERIFY("idle_verify", IdleVerifyCommandArg.class, "Verify counters and hash sums of primary and backup partitions for the specified caches/cache groups on an idle cluster and print out the differences, if any. " +
        "Cache filtering options configure the set of caches that will be processed by idle_verify command. " +
        "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " + EXCLUDE_CACHES + " is empty set. " +
        "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently filtered by cache name " +
        "regexps, by cache type and after all by exclude regexps.", new IdleVerify()),

    /**
     * Prints info regarding caches, groups or sequences.
     */
    LIST("list", ListCommandArg.class,
        "Show information about caches, groups or sequences that match a regular expression. " +
            "When executed without parameters, this subcommand prints the list of caches.", new CacheViewer()),

    /**
     * Validates indexes attempting to read each indexed entry.
     */
    VALIDATE_INDEXES("validate_indexes", ValidateIndexesCommandArg.class,
        "Verify counters and hash sums of primary and backup partitions for the specified caches/cache groups on an idle cluster and print out the differences, if any. " +
            "Cache filtering options configure the set of caches that will be processed by " + IDLE_VERIFY + " command. " +
            "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " + EXCLUDE_CACHES + " is empty set. " +
            "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently filtered by cache name " +
            "regexps, by cache type and after all by exclude regexps.", new CacheValidateIndexes()),

    /**
     * Prints info about contended keys (the keys concurrently locked from multiple transactions).
     */
    CONTENTION("contention", null, "Show the keys that are point of contention for multiple transactions.",
        new CacheContention()),

    /**
     * Collect information on the distribution of partitions.
     */
    DISTRIBUTION("distribution", DistributionCommandArg.class, "Prints the information about partition distribution.",
        new CacheDistribution()),

    /**
     * Reset lost partitions
     */
    RESET_LOST_PARTITIONS("reset_lost_partitions", null, "Reset the state of lost partitions for the specified caches.",
        new ResetLostPartitions()),

    /**
     * Find and remove garbage.
     */
    FIND_AND_DELETE_GARBAGE("find_garbage", FindAndDeleteGarbageArg.class,
        "Find and optionally delete garbage from shared cache groups which could be left after cache destroy.",
        new FindAndDeleteGarbage());


    /** Enumerated values. */
    private static final CacheSubcommands[] VALS = values();

    /** Enum class with argument list for command. */
    private final Class<? extends Enum<? extends CommandArg>> commandArgs;

    /** Name. */
    private final String name;

    /** Command description. */
    private final String desc;

    /** Command instance for certain type. */
    private final Command command;

    /**
     * @param name Name.
     * @param desc Command description.
     * @param command Command realization.
     */
    CacheSubcommands(
        String name,
        Class<? extends Enum<? extends CommandArg>> commandArgs,
        String desc,
        Command command
    ) {
        this.name = name;
        this.commandArgs = commandArgs;
        this.desc = desc;
        this.command = command;
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
     * @return Command description.
     */
    public String description() {
        return desc;
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
