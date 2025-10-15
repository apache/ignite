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
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link TransactionIsolation}.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 */
public class TransactionIsolationMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 502;

    /** Transaction isolation. */
    private TransactionIsolation val;

    /** Code. */
    @Order(0)
    private byte code = -1;

    /** Constructor. */
    public TransactionIsolationMessage() {
        // No-op.
    }

    /** Constructor. */
    public TransactionIsolationMessage(TransactionIsolation val) {
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

    /** @return Transaction isolation. */
    public TransactionIsolation value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /**
     * @param val Transaction isolation.
     * @return Code.
     */
    private byte code(@Nullable TransactionIsolation val) {
        if (val == null)
            return -1;

        switch (val) {
            case READ_COMMITTED:
                return 0;

            case REPEATABLE_READ:
                return 1;

            case SERIALIZABLE:
                return 2;

            default:
                throw new IllegalArgumentException("Unknown transaction isolation value: " + val);
        }
    }

    /**
     * @param code Code.
     * @return Transaction isolation or null.
     */
    @Nullable private TransactionIsolation value(byte code) {
        switch (code) {
            case -1:
                return null;

            case 0:
                return TransactionIsolation.READ_COMMITTED;

            case 1:
                return TransactionIsolation.REPEATABLE_READ;

            case 2:
                return TransactionIsolation.SERIALIZABLE;

            default:
                throw new IllegalArgumentException("Unknown transaction isolation code: " + code);
        }
    }
}
