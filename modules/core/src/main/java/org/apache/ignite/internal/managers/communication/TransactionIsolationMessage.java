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

/**
 * Message for {@link TransactionIsolation}.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 * <p>
 * The values test is TransactionIsolationMessageTest.
 */
public class TransactionIsolationMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 502;

    /** Transaction isolation. */
    @Order(value = 0, method = "code", asType = "byte")
    private TransactionIsolation val;

    /** Constructor. */
    public TransactionIsolationMessage() {
        // No-op.
    }

    /** Constructor. */
    public TransactionIsolationMessage(TransactionIsolation val) {
        this.val = val;
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
     * @return Code.
     */
    public byte code() {
        return val == null ? -1 : (byte)val.ordinal();
    }

    /**
     * @param code Code.
     */
    public void code(byte code) {
        val = TransactionIsolation.fromOrdinal(code);
    }
}
