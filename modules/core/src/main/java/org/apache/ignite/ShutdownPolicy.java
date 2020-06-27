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

package org.apache.ignite;

public enum ShutdownPolicy {
    /**
     * Stop immediately as soon as all components are ready.
     */
    IMMEDIATE(0),

    /**
     * Node will stop if and only if it does not store any unique partitions, that does not have copies on cluster.
     */
    GRACEFUL(1);

    /** Index for serialization. Should be consistent throughout all versions. */
    private final int idx;

    /**
     * @param idx Value index.
     */
    ShutdownPolicy(int idx) {
        this.idx = idx;
    }

    /**
     * @return Index for serialization.
     */
    public int index() {
        return idx;
    }

    /** Enumerated values. */
    private static final ShutdownPolicy[] VALS;

    static {
        ShutdownPolicy[] policyTypes = ShutdownPolicy.values();

        int maxIdx = 0;
        for (ShutdownPolicy recordType : policyTypes)
            maxIdx = Math.max(maxIdx, recordType.idx);

        VALS = new ShutdownPolicy[maxIdx + 1];

        for (ShutdownPolicy policyType : policyTypes)
            VALS[policyType.idx] = policyType;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    public static ShutdownPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * Return shutdown policy matching to string.
     *
     * @param val String representation.
     * @return Shutdown policy.
     */
    public ShutdownPolicy fromString(String val) {
        switch (val) {
            case "IMMEDIATE":
                return IMMEDIATE;

            case "GRACEFUL":
                return GRACEFUL;

            default:
                throw new IllegalArgumentException(val);
        }
    }
}
