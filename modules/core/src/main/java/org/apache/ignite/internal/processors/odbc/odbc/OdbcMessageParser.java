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

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryThreadLocalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;

/**
 * JDBC message parser.
 */
public class OdbcMessageParser implements SqlListenerMessageParser {
    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Kernal context. */
    protected GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public OdbcMessageParser(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        if (ctx.cacheObjects() instanceof CacheObjectBinaryProcessorImpl) {
            CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

            marsh = cacheObjProc.marshaller();
        }
        else {
            throw new IgniteException("ODBC can only be used with BinaryMarshaller (please set it " +
                "through IgniteConfiguration.setMarshaller())");
        }
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(marsh.context(), stream, ctx.config().getClassLoader(), true);

        byte cmd = reader.readByte();

        SqlListenerRequest res;

        switch (cmd) {
            case OdbcRequest.QRY_EXEC: {
                String schema = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = SqlListenerUtils.readObject(reader, true);

                res = new OdbcQueryExecuteRequest(schema, sql, params);

                break;
            }

            case OdbcRequest.QRY_FETCH: {
                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new OdbcQueryFetchRequest(queryId, pageSize);

                break;
            }

            case OdbcRequest.QRY_CLOSE: {
                long queryId = reader.readLong();

                res = new OdbcQueryCloseRequest(queryId);

                break;
            }

            case OdbcRequest.META_COLS: {
                String schema = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                res = new OdbcQueryGetColumnsMetaRequest(schema, table, column);

                break;
            }

            case OdbcRequest.META_TBLS: {
                String catalog = reader.readString();
                String schema = reader.readString();
                String table = reader.readString();
                String tableType = reader.readString();

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            case OdbcRequest.META_PARAMS: {
                String schema = reader.readString();
                String sqlQuery = reader.readString();

                res = new OdbcQueryGetParamsMetaRequest(schema, sqlQuery);

                break;
            }

            default:
                throw new IgniteException("Unknown ODBC command: [cmd=" + cmd + ']');
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(SqlListenerResponse msg0) {
        assert msg0 != null;

        assert msg0 instanceof OdbcResponse;

        OdbcResponse msg = (OdbcResponse)msg0;

        // Creating new binary writer
        BinaryWriterExImpl writer = new BinaryWriterExImpl(marsh.context(), new BinaryHeapOutputStream(INIT_CAP),
            BinaryThreadLocalContext.get().schemaHolder(), null);

        // Writing status.
        writer.writeByte((byte) msg.status());

        if (msg.status() != SqlListenerResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return writer.array();
        }

        Object res0 = msg.response();

        if (res0 == null)
            return writer.array();
        else if (res0 instanceof OdbcQueryExecuteResult) {
            OdbcQueryExecuteResult res = (OdbcQueryExecuteResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());

            Collection<OdbcColumnMeta> metas = res.getColumnsMetadata();

            assert metas != null;

            writer.writeInt(metas.size());

            for (OdbcColumnMeta meta : metas)
                meta.write(writer);
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

                    for (Object obj : row)
                        SqlListenerUtils.writeObject(writer, obj, true);
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

            SqlListenerUtils.writeObject(writer, typeIds, true);
        }
        else
            assert false : "Should not reach here.";

        return writer.array();
    }
}
