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

/** JDBC transaction savepoint request. */
public class JdbcTxSavepointRequest extends JdbcRequest {
    /** Create savepoint operation. */
    public static final byte SAVEPOINT = 0;

    /** Rollback to savepoint operation. */
    public static final byte ROLLBACK_TO_SAVEPOINT = 1;

    /** Release savepoint operation. */
    public static final byte RELEASE_SAVEPOINT = 2;

    /** Transaction id. */
    private int txId;

    /** Operation. */
    private byte op;

    /** Savepoint name. */
    private String name;

    /** Default constructor is used for deserialization. */
    public JdbcTxSavepointRequest() {
        super(TX_SAVEPOINT);
    }

    /**
     * @param txId Transaction id.
     * @param op Operation.
     * @param name Savepoint name.
     */
    public JdbcTxSavepointRequest(int txId, byte op, String name) {
        this();

        this.txId = txId;
        this.op = op;
        this.name = name;
    }

    /** @return Transaction id. */
    public int txId() {
        return txId;
    }

    /** @return Operation. */
    public byte operation() {
        return op;
    }

    /** @return Savepoint name. */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterEx writer, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeInt(txId);
        writer.writeByte(op);
        writer.writeString(name);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderEx reader, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        txId = reader.readInt();
        op = reader.readByte();
        name = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTxSavepointRequest.class, this);
    }
}
