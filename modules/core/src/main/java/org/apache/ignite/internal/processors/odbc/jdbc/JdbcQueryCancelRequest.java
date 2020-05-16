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
 * JDBC query cancel request.
 */
public class JdbcQueryCancelRequest extends JdbcRequest {
    /** Id of a request to be cancelled. */
    private long reqIdToCancel;

    /**
     */
    public JdbcQueryCancelRequest() {
        super(QRY_CANCEL);
    }

    /**
     * @param reqIdToCancel Id of a request to be cancelled.
     */
    public JdbcQueryCancelRequest(long reqIdToCancel) {
        super(QRY_CANCEL);

        this.reqIdToCancel = reqIdToCancel;
    }

    /**
     * @return Id of a request to be cancelled.
     */
    public long requestIdToBeCancelled() {
        return reqIdToCancel;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeLong(reqIdToCancel);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        reqIdToCancel = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryCancelRequest.class, this);
    }
}
