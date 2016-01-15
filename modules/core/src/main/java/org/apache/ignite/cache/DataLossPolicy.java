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

import org.apache.ignite.events.EventType;
import org.jetbrains.annotations.Nullable;

/**
 * Cache policies for a lost partition.
 */
public enum DataLossPolicy {
    /**
     * Specifies no operation policy for a lost partition. Only {@link EventType#EVT_CACHE_REBALANCE_PART_DATA_LOST}
     * is posted. This is a default behaviour.
     */
    NOOP,

    /**
     * Specifies failing operation policy for a lost partition. In this mode all requests for keys from a lost partition
     * fail with an exception until partition state is reset.
     */
    FAIL_OPS;

    /** Enumerated values. */
    private static final DataLossPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static DataLossPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
