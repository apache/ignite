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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Superclass message for all enum message wrappers.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 * @param <T> Type of wrapped enum.
 */
public abstract class EnumMessage<T extends Enum<T>> implements Message {
    /** Enum value. */
    private @Nullable T val;

    /** Code of enum value. */
    @Order(0)
    private byte code = -1;

    /**
     * Default constructor.
     */
    protected EnumMessage() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    protected EnumMessage(@Nullable T val) {
        this.val = val;
        code = code(val);
    }

    /** @return Code. */
    public byte code() {
        return code;
    }

    /** @param code Code. */
    public void code(byte code) {
        this.code = code;
        val = value(code);
    }

    /**
     * @return Enum value.
     */
    public @Nullable T value() {
        return val;
    }

    /**
     * Determines, whether wrapped enum has specified value.
     *
     * @param otherVal Other value.
     */
    public boolean is(Enum<T> otherVal) {
        return val == otherVal;
    }

    /**
     * @param val Value.
     *
     * @return Code of specified value or <code>-1</code> for <code>null</code>.
     * @throws IllegalArgumentException If unexpeced value was passed.
     */
    private byte code(@Nullable T val) {
        if (val == null)
            return -1;

        return code0(val);
    }

    /**
     * @param code Code.
     *
     * @return Value for code or null for <code>-1</code>.
     * @throws IllegalArgumentException If unexpeced code was passed.
     */
    private @Nullable T value(byte code) {
        if (code == -1)
            return null;

        return value0(code);
    }

    /**
     * @param val Value.
     *
     * @return Code of specified value.
     * @throws IllegalArgumentException If unexpeced value was passed.
     */
    protected abstract byte code0(T val);

    /**
     * @param code Code.
     * @return Value for code.
     * @throws IllegalArgumentException If unexpeced code was passed.
     */
    protected abstract T value0(byte code);
}
