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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;

import java.util.Collection;
import org.jetbrains.annotations.NotNull;

/**
 * ODBC message parser.
 */
public class OdbcMessageParser {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Logger. */
    private final IgniteLogger log;

    /** Protocol version confirmation flag. */
    private boolean verConfirmed = false;

    /**
     * @param ctx Context.
     */
    public OdbcMessageParser(final GridKernalContext ctx) {
        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

        this.marsh = cacheObjProc.marshaller();

        this.log = ctx.log(getClass());
    }

    /**
     * Decode OdbcRequest from byte array.
     *
     * @param msg Message.
     * @return Assembled ODBC request.
     */
    public OdbcRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null, true);

        byte cmd = reader.readByte();

        // This is a special case because we can not decode protocol messages until
        // we has not confirmed that the remote client uses the same protocol version.
        if (!verConfirmed) {
            if (cmd == OdbcRequest.HANDSHAKE)
            {
                long longVersion = reader.readLong();

                OdbcHandshakeRequest res = new OdbcHandshakeRequest(longVersion);

                OdbcProtocolVersion version = res.version();

                if (version.isUnknown())
                    return res;

                if (version.isDistributedJoinsSupported()) {
                    res.distributedJoins(reader.readBoolean());
                    res.enforceJoinOrder(reader.readBoolean());
                }

                return res;
            }
            else
                throw new IgniteException("Unexpected ODBC command " +
                        "(first message is not a handshake request): [cmd=" + cmd + ']');
        }

        OdbcRequest res;
        
        switch (cmd) {
            case OdbcRequest.EXECUTE_SQL_QUERY: {
                String cache = reader.readString();
                String sql = reader.readString();
                int paramNum = reader.readInt();

                Object[] params = readParameterRow(reader, paramNum);

                res = new OdbcQueryExecuteRequest(cache, sql, params);

                break;
            }

            case OdbcRequest.QRY_EXEC_BATCH: {
                String schema = reader.readString();
                String sql = reader.readString();
                int paramRowLen = reader.readInt();
                int rowNum = reader.readInt();
                boolean last = reader.readBoolean();

                Object[][] params = new Object[rowNum][];

                for (int i = 0; i < rowNum; ++i)
                    params[i] = readParameterRow(reader, paramRowLen);

                res = new OdbcQueryExecuteBatchRequest(schema, sql, last, params);

                break;
            }

            case OdbcRequest.FETCH_SQL_QUERY: {
                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new OdbcQueryFetchRequest(queryId, pageSize);

                break;
            }

            case OdbcRequest.CLOSE_SQL_QUERY: {
                long queryId = reader.readLong();

                res = new OdbcQueryCloseRequest(queryId);

                break;
            }

            case OdbcRequest.GET_COLUMNS_META: {
                String cache = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                res = new OdbcQueryGetColumnsMetaRequest(cache, table, column);

                break;
            }

            case OdbcRequest.GET_TABLES_META: {
                String catalog = reader.readString();
                String schema = reader.readString();
                String table = reader.readString();
                String tableType = reader.readString();

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            case OdbcRequest.GET_PARAMS_META: {
                String cacheName = reader.readString();
                String sqlQuery = reader.readString();

                res = new OdbcQueryGetParamsMetaRequest(cacheName, sqlQuery);

                break;
            }

            default:
                throw new IgniteException("Unknown ODBC command: [cmd=" + cmd + ']');
        }

        return res;
    }

    /**
     * Read row of parameters using reader.
     * @param reader reader
     * @param paramNum Number of parameters in a row
     * @return Parameters array.
     */
    @NotNull private static Object[] readParameterRow(BinaryReaderExImpl reader, int paramNum) {
        Object[] params = new Object[paramNum];

        for (int i = 0; i < paramNum; ++i)
            params[i] = reader.readObjectDetached();

        return params;
    }

    /**
     * Encode OdbcResponse to byte array.
     *
     * @param msg Message.
     * @return Byte array.
     */
    public byte[] encode(OdbcResponse msg) {
        assert msg != null;

        // Creating new binary writer
        BinaryWriterExImpl writer = marsh.writer(new BinaryHeapOutputStream(INIT_CAP));

        // Writing status.
        writer.writeByte((byte) msg.status());

        if (msg.status() != OdbcResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return writer.array();
        }

        Object res0 = msg.response();

        if (res0 == null)
            return writer.array();
        if (res0 instanceof OdbcHandshakeResult) {
            OdbcHandshakeResult res = (OdbcHandshakeResult) res0;

            if (log.isDebugEnabled())
                log.debug("Handshake result: " + (res.accepted() ? "accepted" : "rejected"));

            verConfirmed = res.accepted();

            if (res.accepted()) {
                verConfirmed = true;

                writer.writeBoolean(true);
            }
            else {
                writer.writeBoolean(false);
                writer.writeString(res.protocolVersionSince());
                writer.writeString(res.currentVersion());
            }
        }
        else if (res0 instanceof OdbcQueryExecuteResult) {
            OdbcQueryExecuteResult res = (OdbcQueryExecuteResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.queryId());

            writer.writeLong(res.queryId());

            Collection<OdbcColumnMeta> metas = res.getColumnsMetadata();

            assert metas != null;

            writer.writeInt(metas.size());

            for (OdbcColumnMeta meta : metas)
                meta.write(writer);
        }
        else if (res0 instanceof OdbcQueryExecuteBatchResult) {
            OdbcQueryExecuteBatchResult res = (OdbcQueryExecuteBatchResult) res0;

            writer.writeBoolean(res.errorMessage() == null);
            writer.writeLong(res.rowsAffected());

            if (res.errorMessage() != null) {
                writer.writeLong(res.errorSetIdx());
                writer.writeString(res.errorMessage());
            }
        }
        else if (res0 instanceof OdbcQueryFetchResult) {
            OdbcQueryFetchResult res = (OdbcQueryFetchResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.queryId());

            writer.writeLong(res.queryId());

            Collection<?> items0 = res.items();

            assert items0 != null;

            writer.writeBoolean(res.last());

            writer.writeInt(items0.size());

            for (Object row0 : items0) {
                if (row0 != null) {
                    Collection<?> row = (Collection<?>)row0;

                    writer.writeInt(row.size());

                    for (Object obj : row) {
                        if (obj instanceof java.sql.Timestamp)
                            writer.writeTimestamp((java.sql.Timestamp)obj);
                        else if (obj instanceof java.util.Date)
                            writer.writeDate((java.util.Date)obj);
                        else
                            writer.writeObjectDetached(obj);
                    }
                }
            }
        }
        else if (res0 instanceof OdbcQueryCloseResult) {
            OdbcQueryCloseResult res = (OdbcQueryCloseResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());
        }
        else if (res0 instanceof OdbcQueryGetColumnsMetaResult) {
            OdbcQueryGetColumnsMetaResult res = (OdbcQueryGetColumnsMetaResult) res0;

            Collection<OdbcColumnMeta> columnsMeta = res.meta();

            assert columnsMeta != null;

            writer.writeInt(columnsMeta.size());

            for (OdbcColumnMeta columnMeta : columnsMeta)
                columnMeta.write(writer);
        }
        else if (res0 instanceof OdbcQueryGetTablesMetaResult) {
            OdbcQueryGetTablesMetaResult res = (OdbcQueryGetTablesMetaResult) res0;

            Collection<OdbcTableMeta> tablesMeta = res.meta();

            assert tablesMeta != null;

            writer.writeInt(tablesMeta.size());

            for (OdbcTableMeta tableMeta : tablesMeta)
                tableMeta.writeBinary(writer);
        }
        else if (res0 instanceof OdbcQueryGetParamsMetaResult) {
            OdbcQueryGetParamsMetaResult res = (OdbcQueryGetParamsMetaResult) res0;

            byte[] typeIds = res.typeIds();

            writer.writeObjectDetached(typeIds);
        }
        else
            assert false : "Should not reach here.";

        return writer.array();
    }
}
