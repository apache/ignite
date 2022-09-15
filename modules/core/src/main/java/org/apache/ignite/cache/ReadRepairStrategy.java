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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Read repair strategies.
 *
 * @see IgniteCache#withReadRepair(ReadRepairStrategy) for details.
 */
@IgniteExperimental
public enum ReadRepairStrategy {
    /** Last write (the newest entry) wins.
     * <p>
     * May cause {@link IgniteException} when fix is impossible (unable to detect the newest entry):
     * <ul>
     * <li>Null(s) found as well as non-null values for the same key.
     * <p>
     * Null (missed entry) has no version, so, it can not be compared with the versioned entry.</li>
     * <li>Entries with the same version have different values.</li>
     * </ul>
     */
    LWW("LWW"),

    /** Value from the primary node wins. */
    PRIMARY("PRIMARY"),

    /** The relative majority, any value found more times than any other wins.
     * <p>
     * Works for an even number of copies (which is typical of Ignite) instead of an absolute majority.
     * <p>
     * May cause {@link IgniteException} when unable to detect values found more times than any other.
     * <p>
     * For example, when we have 5 copies (4 backups) and value `A` is found twice, but `X`, `Y`, and `Z` only once, `A` wins.
     * But, when `A` is found twice, as well as `B`, and `X` only once, the strategy is unable to determine the winner.
     * <p>
     * When we have 4 copies (3 backups), any value found two or more times, when others are found only once, is the winner.
     */
    RELATIVE_MAJORITY("RELATIVE_MAJORITY"),

    /** Inconsistent entries will be removed. */
    REMOVE("REMOVE"),

    /** Only check will be performed. */
    CHECK_ONLY("CHECK_ONLY");

    /** Strategy name. */
    private final String name;

    /**
     * @param name Name.
     */
    ReadRepairStrategy(String name) {
        this.name = name;
    }

    /**
     * Provides strategy by name.
     *
     * @param name Text.
     * @return Read Repair strategy.
     */
    public static ReadRepairStrategy fromString(String name) {
        for (ReadRepairStrategy strategy : values()) {
            if (strategy.name.equalsIgnoreCase(name))
                return strategy;
        }

        throw new IllegalArgumentException("Unknown strategy: " + name);
    }
}
