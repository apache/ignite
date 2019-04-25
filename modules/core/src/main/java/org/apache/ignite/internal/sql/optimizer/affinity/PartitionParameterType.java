/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.jetbrains.annotations.Nullable;

/**
 * Partition argument type.
 */
public enum PartitionParameterType {
    /** Boolean. */
    BOOLEAN,

    /** Byte. */
    BYTE,

    /** Short. */
    SHORT,

    /** Int. */
    INT,

    /** Long. */
    LONG,

    /** Float. */
    FLOAT,

    /** Double. */
    DOUBLE,

    /** String. */
    STRING,

    /** Decimal. */
    DECIMAL,

    /** UUID. */
    UUID;

    /** Enumerated values. */
    private static final PartitionParameterType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static PartitionParameterType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
