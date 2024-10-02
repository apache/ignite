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
import org.apache.ignite.internal.MarshallerPlatformIds;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC get binary type name request.
 */
public class JdbcBinaryTypeNameGetRequest extends JdbcRequest {
    /** ID of binary type. */
    private int typeId;

    /** ID of platform. */
    private byte platformId;

    /** Default constructor for deserialization purpose. */
    JdbcBinaryTypeNameGetRequest() {
        super(BINARY_TYPE_NAME_GET);
    }

    /**
     * @param typeId ID of binary type.
     * @param platformId ID of platform. See {@link MarshallerPlatformIds} for supported values.
     */
    public JdbcBinaryTypeNameGetRequest(int typeId, byte platformId) {
        super(BINARY_TYPE_NAME_GET);

        this.typeId = typeId;
        this.platformId = platformId;
    }

    /**
     * Returns ID of binary type.
     *
     * @return ID of binary type.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * Returns ID of platform.
     *
     * @return ID of platform.
     */
    public byte platformId() {
        return platformId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeInt(typeId);
        writer.writeByte(platformId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        typeId = reader.readInt();
        platformId = reader.readByte();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBinaryTypeNameGetRequest.class, this);
    }
}
