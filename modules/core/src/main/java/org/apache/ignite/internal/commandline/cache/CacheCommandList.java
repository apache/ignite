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
public enum CacheCommandList {
    /**
     * Prints out help for the cache command.
     */
    HELP("help", null, "Print how to use other cache commands."),

    /**
     * Checks consistency of primary and backup partitions assuming no concurrent updates are happening in the cluster.
     */
    IDLE_VERIFY("idle_verify", IdleVerifyCommandArg.class, "Verify counters and hash sums of primary and backup partitions for the specified caches/cache groups on an idle cluster and print out the differences, if any. " +
        "Cache filtering options configure the set of caches that will be processed by idle_verify command. " +
        "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " + EXCLUDE_CACHES + " is empty set. " +
        "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently filtered by cache name " +
        "regexps, by cache type and after all by exclude regexps."),

    /**
     * Prints info regarding caches, groups or sequences.
     */
    LIST("list", ListCommandArg.class,
        "Show information about caches, groups or sequences that match a regular expression. " +
            "When executed without parameters, this subcommand prints the list of caches."),

    /**
     * Validates indexes attempting to read each indexed entry.
     */
    VALIDATE_INDEXES("validate_indexes", ValidateIndexesCommandArg.class,
        "Verify counters and hash sums of primary and backup partitions for the specified caches/cache groups on an idle cluster and print out the differences, if any. " +
            "Cache filtering options configure the set of caches that will be processed by " + IDLE_VERIFY + " command. " +
            "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " + EXCLUDE_CACHES + " is empty set. " +
            "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently filtered by cache name " +
            "regexps, by cache type and after all by exclude regexps."),

    /**
     * Prints info about contended keys (the keys concurrently locked from multiple transactions).
     */
    CONTENTION("contention", null, "Show the keys that are point of contention for multiple transactions."),

    /**
     * Collect information on the distribution of partitions.
     */
    DISTRIBUTION("distribution", DistributionCommandArg.class, "Prints the information about partition distribution."),

    /**
     * Reset lost partitions
     */
    RESET_LOST_PARTITIONS("reset_lost_partitions", null, "Reset the state of lost partitions for the specified caches."),

    /**
     * Find and remove garbage.
     */
    FIND_AND_DELETE_GARBAGE("find_garbage", FindAndDeleteGarbageArg.class,
        "Find and optionally delete garbage from shared cache groups which could be left after cache destroy.");


    /** Enumerated values. */
    private static final CacheCommandList[] VALS = values();

    private final Class<? extends Enum<? extends CommandArg>> commandArgs;

    /** Name. */
    private final String name;
    private final String desc;

    /**
     * @param name Name.
     * @param desc Command description.
     */
    CacheCommandList(String name, Class<? extends Enum<? extends CommandArg>> commandArgs, String desc) {
        this.name = name;
        this.commandArgs = commandArgs;
        this.desc = desc;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static CacheCommandList of(String text) {
        for (CacheCommandList cmd : CacheCommandList.values()) {
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

    public String description() {
        return desc;
    }

    /**
     * @return
     */
    public Class<? extends Enum<? extends CommandArg> > getCommandArgs() {
        return commandArgs;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheCommandList fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
