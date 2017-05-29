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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * SQL listener command request.
 */
public class SqlListenerRequest implements RawBinarylizable {
    /** Handshake request. */
    public static final int HANDSHAKE = 1;

    /** Execute sql query. */
    public static final int QRY_EXEC = 2;

    /** Fetch query results. */
    public static final int QRY_FETCH = 3;

    /** Gather query metadata. */
    public static final int QRY_METADATA = 4;

    /** Close query. */
    public static final int QRY_CLOSE = 5;

    /** Get columns meta query. */
    public static final int META_COLS = 6;

    /** Get columns meta query. */
    public static final int META_TBLS = 7;

    /** Get parameters meta. */
    public static final int META_PARAMS = 8;

    /** Command. */
    private int cmd;

    /** Request ID. */
    private long reqId;

    /**
     * @param cmd Command type.
     */
    public SqlListenerRequest(int cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public int command() {
        return cmd;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId Request ID.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        writer.writeByte((byte)cmd);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        // No-op.
    }

    /**
     * @param reader Binary reader.
     * @return Request object.
     * @throws BinaryObjectException On error.
     */
    public static SqlListenerRequest readRequest(BinaryReaderExImpl reader, SqlListenerAbstractObjectReader objReader)
        throws BinaryObjectException {
        int reqId = reader.readByte();

        SqlListenerRequest req;

        switch(reqId) {
            case QRY_EXEC:
                req = new SqlListenerQueryExecuteRequest();
                break;

            case QRY_FETCH:
                req = new SqlListenerQueryFetchRequest();
                break;

            case QRY_METADATA:
                req = new SqlListenerQueryMetadataRequest();
                break;

            case QRY_CLOSE:
                req = new SqlListenerQueryCloseRequest();
                break;

            case META_COLS:
                req = new OdbcQueryGetColumnsMetaRequest();
                break;

            case META_TBLS:
                req = new OdbcQueryGetTablesMetaRequest();
                break;

            case META_PARAMS:
                req = new OdbcQueryGetParamsMetaRequest();
                break;

            default:
                throw new IgniteException("Unknown SQL listener request ID: [request ID=" + reqId + ']');
        }

        req.readBinary(reader, objReader);

        return req;
    }
}
