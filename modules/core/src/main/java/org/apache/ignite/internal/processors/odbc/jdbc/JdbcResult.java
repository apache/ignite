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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC response result.
 */
public class JdbcResult implements JdbcRawBinarylizable {
    /** Execute sql result. */
    static final byte QRY_EXEC = 2;

    /** Fetch query results. */
    static final byte QRY_FETCH = 3;

    /** Query result's columns metadata result. */
    static final byte QRY_META = 5;

    /** Batch queries. */
    public static final byte BATCH_EXEC = 6;

    /** Tables metadata result. */
    static final byte META_TABLES = 7;

    /** Columns metadata result. */
    static final byte META_COLUMNS = 8;

    /** Indexes metadata result. */
    static final byte META_INDEXES = 9;

    /** SQL query parameters metadata result. */
    static final byte META_PARAMS = 10;

    /** Primary keys metadata result. */
    static final byte META_PRIMARY_KEYS = 11;

    /** Database schemas metadata result. */
    static final byte META_SCHEMAS = 12;

    /** Multiple statements query results. */
    static final byte QRY_EXEC_MULT = 13;

    /** Columns metadata result V2. */
    static final byte META_COLUMNS_V2 = 14;

    /** Columns metadata result V3. */
    static final byte META_COLUMNS_V3 = 15;

    /** A request to send file from client to server. */
    static final byte BULK_LOAD_ACK = 16;

    /** Columns metadata result V4. */
    static final byte META_COLUMNS_V4 = 17;

    /** A result of the processing ordered batch request. */
    static final byte BATCH_EXEC_ORDERED = 18;

    /** A result of the processing cache partitions distributions request. */
    static final byte CACHE_PARTITIONS = 19;

    /** A result of the successfully updated binary schema.  */
    static final byte UPDATE_BINARY_SCHEMA_ACK = 20;

    /** Get binary type schema result. */
    static final byte BINARY_TYPE_GET = 21;

    /** Get binary type name result. */
    static final byte BINARY_TYPE_NAME_GET = 22;

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
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        writer.writeByte(type);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
    }

    /**
     * @param reader Binary reader.
     * @param protoCtx Binary context.
     * @return Request object.
     * @throws BinaryObjectException On error.
     */
    public static JdbcResult readResult(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        int resId = reader.readByte();

        JdbcResult res;

        switch (resId) {
            case QRY_EXEC:
                res = new JdbcQueryExecuteResult();

                break;

            case QRY_FETCH:
                res = new JdbcQueryFetchResult();

                break;

            case QRY_META:
                res = new JdbcQueryMetadataResult();

                break;

            case BATCH_EXEC:
                res = new JdbcBatchExecuteResult();

                break;

            case META_TABLES:
                res = new JdbcMetaTablesResult();

                break;

            case META_COLUMNS:
                res = new JdbcMetaColumnsResult();

                break;

            case META_INDEXES:
                res = new JdbcMetaIndexesResult();

                break;

            case META_PARAMS:
                res = new JdbcMetaParamsResult();

                break;

            case META_PRIMARY_KEYS:
                res = new JdbcMetaPrimaryKeysResult();

                break;

            case META_SCHEMAS:
                res = new JdbcMetaSchemasResult();

                break;

            case QRY_EXEC_MULT:
                res = new JdbcQueryExecuteMultipleStatementsResult();

                break;

            case META_COLUMNS_V2:
                res = new JdbcMetaColumnsResultV2();

                break;

            case META_COLUMNS_V3:
                res = new JdbcMetaColumnsResultV3();

                break;

            case BULK_LOAD_ACK:
                res = new JdbcBulkLoadAckResult();

                break;

            case META_COLUMNS_V4:
                res = new JdbcMetaColumnsResultV4();

                break;

            case BATCH_EXEC_ORDERED:
                res = new JdbcOrderedBatchExecuteResult();

                break;

            case CACHE_PARTITIONS:
                res = new JdbcCachePartitionsResult();

                break;

            case UPDATE_BINARY_SCHEMA_ACK:
                res = new JdbcUpdateBinarySchemaResult();

                break;

            case BINARY_TYPE_GET:
                res = new JdbcBinaryTypeGetResult();

                break;

            case BINARY_TYPE_NAME_GET:
                res = new JdbcBinaryTypeNameGetResult();

                break;

            default:
                throw new IgniteException("Unknown SQL listener request ID: [request ID=" + resId + ']');
        }

        res.readBinary(reader, protoCtx);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcResult.class, this);
    }
}
