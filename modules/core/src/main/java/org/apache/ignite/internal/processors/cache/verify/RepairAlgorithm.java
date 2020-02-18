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

package org.apache.ignite.internal.processors.cache.verify;

import org.jetbrains.annotations.Nullable;

/**
 * Partition reconciliation repair algorithm options for fixing doubtful keys
 * (in practice doubtful usually means fully removed keys that cannot be found in deferred delete queue).
 */
public enum RepairAlgorithm {
    /** Value from entry with max grid cache version will be used.*/
    LATEST,

    /** Value from entry from primary node will be used. */
    PRIMARY,

    /** Most common value will be used. */
    MAJORITY,

    /** Does remove a key form all partitions. */
    REMOVE,

    /** Nicht repair, only printing. */
    PRINT_ONLY;

    /** Enumerated values. */
    private static final RepairAlgorithm[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static RepairAlgorithm fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @return Default repair algorithm.
     */
    public static RepairAlgorithm defaultValue() {
        return PRINT_ONLY;
    }

}
