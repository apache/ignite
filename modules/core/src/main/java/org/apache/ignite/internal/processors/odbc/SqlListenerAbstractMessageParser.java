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

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * ODBC message parser.
 */
public abstract class SqlListenerAbstractMessageParser implements SqlListenerMessageParser {
    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Kernal context. */
    protected GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Object reader. */
    private SqlListenerAbstractObjectReader objReader;

    /** Object writer. */
    private SqlListenerAbstractObjectWriter objWriter;

    /**
     * @param ctx Context.
     * @param objReader Object reader.
     * @param objWriter Object writer.
     */
    protected SqlListenerAbstractMessageParser(final GridKernalContext ctx, SqlListenerAbstractObjectReader objReader,
        SqlListenerAbstractObjectWriter objWriter) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        this.objReader = objReader;
        this.objWriter = objWriter;
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryReaderExImpl reader = createReader(msg);

        byte cmd = reader.readByte();

        SqlListenerRequest res;

        switch (cmd) {
            case SqlListenerRequest.QRY_EXEC: {
                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = objReader.readObject(reader);

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
        BinaryWriterExImpl writer = createWriter(INIT_CAP);

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

                    for (Object obj : row)
                        objWriter.writeObject(writer, obj);
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

            objWriter.writeObject(writer, typeIds);
        }
        else
            assert false : "Should not reach here.";

        return writer.array();
    }

    /**
     * Create reader.
     *
     * @param msg Input message.
     * @return Reader.
     */
    protected abstract BinaryReaderExImpl createReader(byte[] msg);

    /**
     * Create writer.
     *
     * @param cap Initial capacity.
     * @return Binary writer instance.
     */
    protected abstract BinaryWriterExImpl createWriter(int cap);
}
