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

/**
 * Read repair strategies.
 *
 * @see IgniteCache#withReadRepair(ReadRepairStrategy) for details.
 */
public enum ReadRepairStrategy {
    /** Last write wins. */
    LWW("LWW"),

    /** Value from the primary node wins. */
    PRIMARY("PRIMARY"),

    /** Majority wins. */
    MAJORITY("MAJORITY"),

    /** Inconsistent entry will be removed. */
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

    /**
     * @return Read Repair strategy.
     */
    public static ReadRepairStrategy defaultStrategy() {
        return LWW;
    }
}
