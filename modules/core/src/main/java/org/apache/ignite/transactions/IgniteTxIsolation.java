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

package org.apache.ignite.transactions;

import org.jetbrains.annotations.*;

/**
 * Defines different cache transaction isolation levels. See {@link IgniteTx}
 * documentation for more information about cache transaction isolation levels.
 */
public enum IgniteTxIsolation {
    /** Read committed isolation level. */
    READ_COMMITTED,

    /** Repeatable read isolation level. */
    REPEATABLE_READ,

    /** Serializable isolation level. */
    SERIALIZABLE;

    /** Enum values. */
    private static final IgniteTxIsolation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static IgniteTxIsolation fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
