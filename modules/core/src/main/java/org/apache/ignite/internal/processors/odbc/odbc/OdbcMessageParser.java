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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientMessage;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
import org.jetbrains.annotations.NotNull;

/**
 * JDBC message parser.
 */
public class OdbcMessageParser implements ClientListenerMessageParser {
    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Protocol version */
    private final ClientListenerProtocolVersion ver;

    /**
     * @param ctx Context.
     * @param ver Protocol version.
     */
    public OdbcMessageParser(GridKernalContext ctx, ClientListenerProtocolVersion ver) {
        this.ctx = ctx;
        this.ver = ver;

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
    @Override public ClientListenerRequest decode(ClientMessage msg) {
        assert msg != null;

        BinaryInputStream stream = BinaryStreams.createHeapInputStream(msg.payload());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(marsh.context(), stream, ctx.config().getClassLoader(), true);

        byte cmd = reader.readByte();

        ClientListenerRequest res;

        switch (cmd) {
            case OdbcRequest.QRY_EXEC: {
                String schema = reader.readString();
                String sql = reader.readString();
                int paramNum = reader.readInt();

                Object[] params = readParameterRow(reader, paramNum);

                int timeout = 0;

                if (ver.compareTo(OdbcConnectionContext.VER_2_3_2) >= 0)
                    timeout = reader.readInt();

                boolean autoCommit = true;

                if (ver.compareTo(OdbcConnectionContext.VER_2_7_0) >= 0)
                    autoCommit = reader.readBoolean();

                res = new OdbcQueryExecuteRequest(schema, sql, params, timeout, autoCommit);

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

                int timeout = 0;

                if (ver.compareTo(OdbcConnectionContext.VER_2_3_2) >= 0)
                    timeout = reader.readInt();

                boolean autoCommit = true;

                if (ver.compareTo(OdbcConnectionContext.VER_2_7_0) >= 0)
                    autoCommit = reader.readBoolean();

                res = new OdbcQueryExecuteBatchRequest(schema, sql, last, params, timeout, autoCommit);

                break;
            }

            case OdbcRequest.STREAMING_BATCH: {
                String schema = reader.readString();

                int num = reader.readInt();

                ArrayList<OdbcQuery> queries = new ArrayList<>(num);

                for (int i = 0; i < num; ++i) {
                    OdbcQuery qry = new OdbcQuery();
                    qry.readBinary(reader);

                    queries.add(qry);
                }

                boolean last = reader.readBoolean();
                long order = reader.readLong();

                res = new OdbcStreamingBatchRequest(schema, queries, last, order);

                break;
            }

            case OdbcRequest.QRY_FETCH: {
                long qryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new OdbcQueryFetchRequest(qryId, pageSize);

                break;
            }

            case OdbcRequest.QRY_CLOSE: {
                long qryId = reader.readLong();

                res = new OdbcQueryCloseRequest(qryId);

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
                String sqlQry = reader.readString();

                res = new OdbcQueryGetParamsMetaRequest(schema, sqlQry);

                break;
            }

            case OdbcRequest.META_RESULTSET: {
                String schema = reader.readString();
                String sqlQry = reader.readString();

                res = new OdbcQueryGetResultsetMetaRequest(schema, sqlQry);

                break;
            }

            case OdbcRequest.MORE_RESULTS: {
                long qryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new OdbcQueryMoreResultsRequest(qryId, pageSize);

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
            params[i] = SqlListenerUtils.readObject(reader, true);

        return params;
    }

    /** {@inheritDoc} */
    @Override public ClientMessage encode(ClientListenerResponse msg0) {
        assert msg0 != null;

        assert msg0 instanceof OdbcResponse;

        OdbcResponse msg = (OdbcResponse)msg0;

        // Creating new binary writer
        BinaryWriterExImpl writer = new BinaryWriterExImpl(
            marsh.context(),
            BinaryStreams.createThreadLocalHeapOutputStream(INIT_CAP),
            null
        );

        // Writing status.
        if (ver.compareTo(OdbcConnectionContext.VER_2_1_5) < 0) {
            writer.writeByte((byte)(msg.status() == ClientListenerResponse.STATUS_SUCCESS ?
                ClientListenerResponse.STATUS_SUCCESS : ClientListenerResponse.STATUS_FAILED));
        }
        else
            writer.writeInt(msg.status());

        if (msg.status() != ClientListenerResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return new ClientMessage(writer.array());
        }

        Object res0 = msg.response();

        if (res0 == null)
            return new ClientMessage(writer.array());
        else if (res0 instanceof OdbcQueryExecuteResult) {
            OdbcQueryExecuteResult res = (OdbcQueryExecuteResult)res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.queryId());

            writer.writeLong(res.queryId());

            Collection<OdbcColumnMeta> metas = res.columnsMetadata();

            writeResultsetMeta(writer, metas);

            writeAffectedRows(writer, res.affectedRows());
        }
        else if (res0 instanceof OdbcQueryExecuteBatchResult) {
            OdbcQueryExecuteBatchResult res = (OdbcQueryExecuteBatchResult)res0;

            writer.writeBoolean(res.errorMessage() == null);
            writeAffectedRows(writer, res.affectedRows());

            if (res.errorMessage() != null) {
                writer.writeLong(res.errorSetIdx());
                writer.writeString(res.errorMessage());

                if (ver.compareTo(OdbcConnectionContext.VER_2_1_5) >= 0)
                    writer.writeInt(res.errorCode());
            }
        }
        else if (res0 instanceof OdbcStreamingBatchResult) {
            OdbcStreamingBatchResult res = (OdbcStreamingBatchResult)res0;

            writer.writeString(res.error());
            writer.writeInt(res.status());
            writer.writeLong(res.order());
        }
        else if (res0 instanceof OdbcQueryFetchResult) {
            OdbcQueryFetchResult res = (OdbcQueryFetchResult)res0;

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
        else if (res0 instanceof OdbcQueryMoreResultsResult) {
            OdbcQueryMoreResultsResult res = (OdbcQueryMoreResultsResult)res0;

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
            OdbcQueryCloseResult res = (OdbcQueryCloseResult)res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());
        }
        else if (res0 instanceof OdbcQueryGetColumnsMetaResult) {
            OdbcQueryGetColumnsMetaResult res = (OdbcQueryGetColumnsMetaResult)res0;

            Collection<OdbcColumnMeta> columnsMeta = res.meta();

            writeResultsetMeta(writer, columnsMeta);
        }
        else if (res0 instanceof OdbcQueryGetTablesMetaResult) {
            OdbcQueryGetTablesMetaResult res = (OdbcQueryGetTablesMetaResult)res0;

            Collection<OdbcTableMeta> tablesMeta = res.meta();

            assert tablesMeta != null;

            writer.writeInt(tablesMeta.size());

            for (OdbcTableMeta tableMeta : tablesMeta)
                tableMeta.writeBinary(writer);
        }
        else if (res0 instanceof OdbcQueryGetParamsMetaResult) {
            OdbcQueryGetParamsMetaResult res = (OdbcQueryGetParamsMetaResult)res0;

            byte[] typeIds = res.typeIds();

            SqlListenerUtils.writeObject(writer, typeIds, true);
        }
        else if (res0 instanceof OdbcQueryGetResultsetMetaResult) {
            OdbcQueryGetResultsetMetaResult res = (OdbcQueryGetResultsetMetaResult)res0;

            writeResultsetMeta(writer, res.columnsMetadata());
        }
        else
            assert false : "Should not reach here.";

        return new ClientMessage(writer.array());
    }

    /**
     * Write resultset columns metadata in a unified way.
     * @param writer Writer.
     * @param meta Metadata
     */
    private void writeResultsetMeta(BinaryWriterExImpl writer, Collection<OdbcColumnMeta> meta) {
        assert meta != null;

        writer.writeInt(meta.size());

        for (OdbcColumnMeta columnMeta : meta)
            columnMeta.write(writer, ver);
    }

    /** {@inheritDoc} */
    @Override public int decodeCommandType(ClientMessage msg) {
        assert msg != null;

        return msg.payload()[0];
    }


    /** {@inheritDoc} */
    @Override public long decodeRequestId(ClientMessage msg) {
        return 0;
    }

    /**
     * @param writer Writer to use.
     * @param affectedRows Affected rows.
     */
    private void writeAffectedRows(BinaryWriterExImpl writer, long[] affectedRows) {
        if (ver.compareTo(OdbcConnectionContext.VER_2_3_2) < 0) {
            long summ = 0;

            for (Long val : affectedRows)
                summ += val == null ? 0 : val;

            writer.writeLong(summ);
        }
        else {
            writer.writeInt(affectedRows.length);

            for (long val : affectedRows)
                writer.writeLong(val);
        }
    }
}
