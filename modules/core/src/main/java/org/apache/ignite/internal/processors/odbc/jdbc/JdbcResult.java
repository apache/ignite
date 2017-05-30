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

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * SQL listener response.
 */
public class JdbcResult implements JdbcRawBinarylizable {
    /** Execute sql result. */
    public static final byte QRY_EXEC = 2;

    /** Fetch query results. */
    public static final byte QRY_FETCH = 3;

    /** Get columns meta query result. */
    public static final byte QRY_META = 4;

    /** Success status. */
    private byte type;

    /**
     * Constructs result.
     *
     * @param type Type of results.
     */
    public JdbcResult(byte type) {
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeByte(type);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        // No-op.
    }

    /**
     * @param reader Binary reader.
     * @return Request object.
     * @throws BinaryObjectException On error.
     */
    public static JdbcResult readResult(BinaryReaderExImpl reader) throws BinaryObjectException {
        int resId = reader.readByte();

        JdbcResult res;

        switch(resId) {
            case QRY_EXEC:
                res = new JdbcQueryExecuteResult();
                break;

            case QRY_FETCH:
                res = new JdbcQueryFetchResult();
                break;

            case QRY_META:
                res = new JdbcQueryMetadataResult();
                break;

            default:
                throw new IgniteException("Unknown SQL listener request ID: [request ID=" + resId + ']');
        }

        res.readBinary(reader);

        return res;
    }
}
