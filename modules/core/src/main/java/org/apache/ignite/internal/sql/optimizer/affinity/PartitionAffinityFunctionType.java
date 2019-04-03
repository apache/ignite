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

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity function type.
 */
public enum PartitionAffinityFunctionType implements JdbcRawBinarylizable {
    /** Custom affintiy function. */
    CUSTOM(0),

    /** Rendezvous affinity function. */
    RENDEZVOUS(1);

    /** Enumerated values. */
    private static final PartitionAffinityFunctionType[] VALS = values();

    /** Value. */
    private final int val;

    /**
     * Constructor.
     *
     * @param val Value.
     */
    PartitionAffinityFunctionType(int val) {
        this.val = val;
    }

    /**
     * @return Value.
     */
    public int value() {
        return val;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static PartitionAffinityFunctionType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    // TODO VO: Move to another place
    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        writer.writeInt(ordinal());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        // No-op.
    }

    /**
     * Returns debinarized partition affinity function type.
     *
     * @param reader Binary reader.
     * @param ver Protocol verssion.
     * @return Debinarized partition affinity function type.
     * @throws BinaryObjectException On error.
     */
    public static PartitionAffinityFunctionType readOperator(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver)
        throws BinaryObjectException {
        return fromOrdinal (reader.readInt());
    }
}
