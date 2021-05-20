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

package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines operation which will be applied to an entry identified by the key.
 * <p>
 * Invariants:
 * <ul>
 *     <li>Any operation identifies a target entry by not null {@code key} except of {@link OperationType#NO_OP}.</li>
 *     <li>Only {@link OperationType#PUT} operation contains value which will be written to meta storage.</li>
 * </ul>
 */
public final class Operation {
    /**
     * Key identifies an entry which operation will be applied to. Key is {@code null} for {@link OperationType#NO_OP} operation.
     */
    @Nullable
    private final byte[] key;

    /**
     * Value which will be associated with the {@link #key}. Value is not {@code null} only for {@link OperationType#PUT}
     * operation.
     */
    @Nullable
    private final byte[] val;

    /**
     * Operation type.
     * @see OperationType
     */
    @NotNull
    private final OperationType type;

    /**
     * Constructs operation which will be applied to an entry identified by the given key.
     *
     * @param type Operation type. Can't be {@code null}.
     * @param key Key identifies an entry which operation will be applied to.
     * @param val Value will be associated with an entry identified by the {@code key}.
     */
    public Operation(@NotNull OperationType type, @Nullable byte[] key, @Nullable byte[] val) {
        assert (type == OperationType.NO_OP && key == null && val == null)
                || (type == OperationType.PUT && key != null && val != null)
                || (type == OperationType.REMOVE && key != null && val == null)
                : "Invalid operation parameters: [type=" + type +
                        ", key=" + (key == null ? "null" : IgniteUtils.toHexString(key, 256)) +
                        ", val=" + (val == null ? "null" : IgniteUtils.toHexString(val, 256)) + ']';

        this.key = key;
        this.val = val;
        this.type = type;
    }

    /**
     * Returns a key which identifies an entry which operation will be applied to.
     *
     * @return A key which identifies an entry which operation will be applied to.
     */
    @Nullable public byte[] key() {
        return key;
    }

    /**
     * Returns a value which will be associated with an entry identified by the {@code key}.
     *
     * @return A value which will be associated with an entry identified by the {@code key}.
     */
    @Nullable public byte[] value() {
        return val;
    }

    /**
     * Returns an operation type.
     *
     * @return An operation type.
     */
    @NotNull public OperationType type() {
        return type;
    }
}
