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

import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link TransactionIsolation}.
 */
public class TransactionIsolationMessage extends EnumMessage<TransactionIsolation> {
    /** Type code. */
    public static final short TYPE_CODE = 502;

    /** Constructor. */
    public TransactionIsolationMessage() {
        // No-op.
    }

    /** Constructor. */
    public TransactionIsolationMessage(TransactionIsolation val) {
        super(val);
    }

    /** {@inheritDoc} */
    @Override protected byte code0(@Nullable TransactionIsolation val) {
        switch (val) {
            case READ_COMMITTED: return 0;
            case REPEATABLE_READ: return 1;
            case SERIALIZABLE: return 2;
        }

        throw new IllegalArgumentException("Unknown transaction isolation value: " + val);
    }

    /** {@inheritDoc} */
    @Override protected TransactionIsolation value0(byte code) {
        switch (code) {
            case 0: return TransactionIsolation.READ_COMMITTED;
            case 1: return TransactionIsolation.REPEATABLE_READ;
            case 2: return TransactionIsolation.SERIALIZABLE;
        }

        throw new IllegalArgumentException("Unknown transaction isolation code: " + code);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
