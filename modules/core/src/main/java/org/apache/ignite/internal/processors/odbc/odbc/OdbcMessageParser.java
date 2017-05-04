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

package org.apache.ignite.internal.processors.odbc.odbc;

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
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetColumnsMetaRequest;
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetColumnsMetaResult;
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetParamsMetaRequest;
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetParamsMetaResult;
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetTablesMetaRequest;
import org.apache.ignite.internal.processors.odbc.OdbcQueryGetTablesMetaResult;
import org.apache.ignite.internal.processors.odbc.OdbcTableMeta;
import org.apache.ignite.internal.processors.odbc.SqlListenerColumnMeta;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryCloseResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryExecuteResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerQueryFetchResult;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;

import java.util.Collection;

/**
 * ODBC message parser.
 */
public class OdbcMessageParser implements SqlListenerMessageParser {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public OdbcMessageParser(final GridKernalContext ctx) {
        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

        this.marsh = cacheObjProc.marshaller();

        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null, true);

        byte cmd = reader.readByte();

        SqlListenerRequest res;

        switch (cmd) {
            case SqlListenerRequest.QRY_EXEC: {
                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = reader.readObjectDetached();

                res = new SqlListenerQueryExecuteRequest(cache, sql, params);

                break;
            }

            case SqlListenerRequest.QRY_FETCH: {
                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new SqlListenerQueryFetchRequest(queryId, pageSize);

                break;
            }

            case SqlListenerRequest.QRY_CLOSE: {
                long queryId = reader.readLong();

                res = new SqlListenerQueryCloseRequest(queryId);

                break;
            }

            case SqlListenerRequest.META_COLS: {
                String cache = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                res = new OdbcQueryGetColumnsMetaRequest(cache, table, column);

                break;
            }

            case SqlListenerRequest.META_TBLS: {
                String catalog = reader.readString();
                String schema = reader.readString();
                String table = reader.readString();
                String tableType = reader.readString();

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            case SqlListenerRequest.META_PARAMS: {
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

    /** {@inheritDoc} */
    @Override public byte[] encode(SqlListenerResponse msg) {
        assert msg != null;

        // Creating new binary writer
        BinaryWriterExImpl writer = marsh.writer(new BinaryHeapOutputStream(INIT_CAP));

        // Writing status.
        writer.writeByte((byte) msg.status());

        if (msg.status() != SqlListenerResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return writer.array();
        }

        Object res0 = msg.response();

        if (res0 == null)
            return writer.array();
        else if (res0 instanceof SqlListenerQueryExecuteResult) {
            SqlListenerQueryExecuteResult res = (SqlListenerQueryExecuteResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());

            Collection<SqlListenerColumnMeta> metas = res.getColumnsMetadata();

            assert metas != null;

            writer.writeInt(metas.size());

            for (SqlListenerColumnMeta meta : metas)
                meta.write(writer);
        }
        else if (res0 instanceof SqlListenerQueryFetchResult) {
            SqlListenerQueryFetchResult res = (SqlListenerQueryFetchResult) res0;

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
                        if (obj == null) {
                            writer.writeObjectDetached(null);
                            continue;
                        }

                        Class<?> cls = obj.getClass();

                        if (cls == java.sql.Time.class)
                            writer.writeTime((java.sql.Time)obj);
                        else if (cls == java.sql.Timestamp.class)
                            writer.writeTimestamp((java.sql.Timestamp)obj);
                        else if (cls == java.sql.Date.class)
                            writer.writeDate((java.util.Date)obj);
                        else
                            writer.writeObjectDetached(obj);
                    }
                }
            }
        }
        else if (res0 instanceof SqlListenerQueryCloseResult) {
            SqlListenerQueryCloseResult res = (SqlListenerQueryCloseResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());
        }
        else if (res0 instanceof OdbcQueryGetColumnsMetaResult) {
            OdbcQueryGetColumnsMetaResult res = (OdbcQueryGetColumnsMetaResult) res0;

            Collection<SqlListenerColumnMeta> columnsMeta = res.meta();

            assert columnsMeta != null;

            writer.writeInt(columnsMeta.size());

            for (SqlListenerColumnMeta columnMeta : columnsMeta)
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
