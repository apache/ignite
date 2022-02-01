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
package org.apache.ignite.internal.processors.query.stat;

/**
 * Statistics usage state.
 */
public enum StatisticsUsageState {
    /**
     *  No statistics used.
     */
    OFF(0),

    /**
     * Statistics used "as is" without updates.
     */
    NO_UPDATE(1),

    /**
     * Statistics used and updated after each changes.
     */
    ON(2);

    /** Index for serialization. Should be consistent throughout all versions. */
    private final int idx;

    /** Enumerated values. */
    private static final StatisticsUsageState[] VALS;

    static {
        StatisticsUsageState[] statisticsUsageStates = StatisticsUsageState.values();

        int maxIdx = 0;
        for (StatisticsUsageState recordType : statisticsUsageStates)
            maxIdx = Math.max(maxIdx, recordType.idx);

        VALS = new StatisticsUsageState[maxIdx + 1];

        for (StatisticsUsageState policyType : statisticsUsageStates)
            VALS[policyType.idx] = policyType;
    }

    /**
     * Constructor.
     *
     * @param idx Value index.
     */
    StatisticsUsageState(int idx) {
        this.idx = idx;
    }

    /**
     * @return Index for serialization.
     */
    public int index() {
        return idx;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    public static StatisticsUsageState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * Return statistics state matching to string.
     *
     * @param val String representation.
     * @return Statistics state.
     */
    public StatisticsUsageState fromString(String val) {
        switch (val) {
            case "OFF":
                return OFF;

            case "NO_UPDATE":
                return NO_UPDATE;

            case "ON":
                return ON;

            default:
                throw new IllegalArgumentException(val);
        }
    }
}
