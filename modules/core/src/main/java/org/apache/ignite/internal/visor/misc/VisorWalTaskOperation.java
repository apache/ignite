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

package org.apache.ignite.internal.visor.misc;

import org.jetbrains.annotations.Nullable;

/**
 *  WAL Task operation types.
 */
public enum VisorWalTaskOperation {
    /** Print unused wal segments. */
    PRINT_UNUSED_WAL_SEGMENTS,

    /** Delete unused wal segments. */
    DELETE_UNUSED_WAL_SEGMENTS;

    /** Enumerated values. */
    private static final VisorWalTaskOperation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static VisorWalTaskOperation fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
