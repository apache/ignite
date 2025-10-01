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

package org.apache.ignite.internal.transactions;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

public enum TransactionIsolationType {
    /** Read committed isolation level. */
    READ_COMMITTED(0),

    /** Repeatable read isolation level. */
    REPEATABLE_READ(1),

    /** Serializable isolation level. */
    SERIALIZABLE(2);

    /** Code. */
    private final int code;

    TransactionIsolationType(int code) {
        this.code = code;
    }

    /**
     * @return Code.
     */
    public short code() {
        return (short)code;
    }

    /**
     * Converts this internal operation code to public {@link TransactionIsolation}.
     *
     * @return Corresponding {@link TransactionIsolationType}, or {@code null} if there is no match.
     */
    @Nullable public TransactionIsolation toPublicIsolationType() {
        switch (this) {
            case READ_COMMITTED:
                return TransactionIsolation.READ_COMMITTED;

            case REPEATABLE_READ:
                return TransactionIsolation.REPEATABLE_READ;

            case SERIALIZABLE:
                return TransactionIsolation.SERIALIZABLE;

            default:
                return null;
        }
    }

    /**
     * Converts this public operation code to internal.
     *
     * @return Corresponding type, or {@code null} if there is no match.
     */
    @Nullable public static TransactionIsolationType toInternalIsolationType(TransactionIsolation isolation) {
        switch (isolation) {
            case READ_COMMITTED:
                return READ_COMMITTED;

            case REPEATABLE_READ:
                return REPEATABLE_READ;

            case SERIALIZABLE:
                return SERIALIZABLE;

            default:
                return null;
        }
    }

    /** Enum mapping from code to values. */
    private static final Map<Short, TransactionIsolationType> map;

    static {
        map = new HashMap<>();

        for (TransactionIsolationType type : values())
            map.put(type.code(), type);
    }

    /**
     * @param code Code to convert to enum.
     * @return Enum.
     */
    @Nullable public static TransactionIsolationType fromCode(short code) {
        return map.get(code);
    }
}
