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
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public enum CacheCommandList {
    /**
     * Prints out help for the cache command.
     */
    HELP("help", null),

    /**
     * Checks consistency of primary and backup partitions assuming no concurrent updates are happening in the cluster.
     */
    IDLE_VERIFY("idle_verify", new IdleVerify()),

    /**
     * Prints info regarding caches, groups or sequences.
     */
    LIST("list", new CacheViewer()),

    /**
     * Validates indexes attempting to read each indexed entry.
     */
    VALIDATE_INDEXES("validate_indexes", new CacheValidateIndexes()),

    /**
     * Check secondary indexes inline size.
     */
    CHECK_INDEX_INLINE_SIZES("check_index_inline_sizes", new CheckIndexInlineSizes()),

    /**
     * Prints info about contended keys (the keys concurrently locked from multiple transactions).
     */
    CONTENTION("contention", new CacheContention()),

    /**
     * Collect information on the distribution of partitions.
     */
    DISTRIBUTION("distribution", new CacheDistribution()),

    /**
     * Reset lost partitions
     */
    RESET_LOST_PARTITIONS("reset_lost_partitions", new ResetLostPartitions()),

    /**
     * Find and remove garbage.
     */
    FIND_AND_DELETE_GARBAGE("find_garbage", new FindAndDeleteGarbage());

    /** Enumerated values. */
    private static final CacheCommandList[] VALS = values();

    /** Name. */
    private final String name;

    /** */
    private final Command command;

    /**
     * @param name Name.
     * @param command Command implementation.
     */
    CacheCommandList(String name, Command command) {
        this.name = name;
        this.command = command;
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

    /**
     * @return Cache subcommand implementation.
     */
    public Command subcommand() {
        return command;
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
