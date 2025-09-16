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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC sets transactions parameters for connection.
 */
public class JdbcSetTxParametersRequest extends JdbcRequest {
    /** Transaction concurrency control. */
    private TransactionConcurrency concurrency;

    /** Transaction isolation level. */
    private @Nullable TransactionIsolation isolation;

    /** Transaction timeout. */
    private int timeout;

    /** Transaction label. */
    private String lb;

    /** Default constructor is used for deserialization. */
    public JdbcSetTxParametersRequest() {
        super(TX_SET_PARAMS);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Isolation level.
     * @param timeout Timeout.
     * @param lb Label.
     */
    public JdbcSetTxParametersRequest(
        TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation,
        int timeout,
        String lb
    ) {
        this();

        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.lb = lb;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterEx writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeByte((byte)concurrency.ordinal());
        writer.writeByte((byte)(isolation == null ? -1 : isolation.ordinal()));
        writer.writeInt(timeout);
        writer.writeString(lb);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderEx reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        concurrency = TransactionConcurrency.fromOrdinal(reader.readByte());

        byte isolationByte = reader.readByte();

        isolation = isolationByte == -1 ? null : TransactionIsolation.fromOrdinal(isolationByte);
        timeout = reader.readInt();
        lb = reader.readString();
    }

    /** */
    public TransactionConcurrency concurrency() {
        return concurrency;
    }

    /** */
    public TransactionIsolation isolation() {
        return isolation;
    }

    /** */
    public long timeout() {
        return timeout;
    }

    /** */
    public String label() {
        return lb;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcSetTxParametersRequest.class, this);
    }
}
