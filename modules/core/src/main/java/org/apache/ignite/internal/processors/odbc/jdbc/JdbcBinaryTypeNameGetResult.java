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
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC get binary type name result.
 */
public class JdbcBinaryTypeNameGetResult extends JdbcResult {
    /** ID of initial request. */
    private long reqId;

    /** Type name. */
    private String typeName;

    /** Default constructor for deserialization purpose. */
    JdbcBinaryTypeNameGetResult() {
        super(BINARY_TYPE_NAME_GET);
    }

    /**
     * @param reqId ID of initial request.
     * @param typeName Name of the binary type.
     */
    public JdbcBinaryTypeNameGetResult(long reqId, String typeName) {
        super(BINARY_TYPE_NAME_GET);

        this.reqId = reqId;
        this.typeName = typeName;
    }

    /**
     * Returns name of the binary type.
     *
     * @return Name of the binary type.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * Returns ID of initial request.
     *
     * @return ID of initial request.
     */
    public long reqId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeLong(reqId);
        writer.writeString(typeName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        reqId = reader.readLong();
        typeName = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBinaryTypeNameGetResult.class, this);
    }
}
