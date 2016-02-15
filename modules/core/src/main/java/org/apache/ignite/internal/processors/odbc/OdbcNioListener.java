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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * SQL query handler.
 */
public class OdbcNioListener extends GridNioServerListenerAdapter<byte[]> {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

    /** Kernel context. */
    private final GridKernalContext ctx;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Logger. */
    private final IgniteLogger log;

    /** Request handler. */
    private final OdbcRequestHandler handler;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public OdbcNioListener(final GridKernalContext ctx, final GridSpinBusyLock busyLock,
                           final OdbcRequestHandler handler) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.handler = handler;

        this.marsh = ((CacheObjectBinaryProcessorImpl)ctx.cacheObjects()).marshaller();

        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("Driver connected");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled())
            log.debug("Driver disconnected");

        if (e != null) {
            if (e instanceof RuntimeException)
                U.error(log, "Failed to process request from remote client: " + ses, e);
            else
                U.warn(log, "Closed client session due to exception [ses=" + ses + ", msg=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, byte[] msg) {
        assert msg != null;

        try {
            OdbcRequest req = decode(msg);

            OdbcResponse rsp = handle(req);

            if (log.isDebugEnabled())
                log.debug("Handling result: [res=" + rsp.status() + ']');

            byte[] outMsg = encode(rsp);

            ses.send(outMsg);
        }
        catch (Exception e) {
            trySendErrorMessage(ses, e.getMessage());
        }
    }

    /**
     * Try to send simple response message to ODBC driver.
     * @param ses Session.
     * @param err Error message.
     */
    private void trySendErrorMessage(GridNioSession ses, String err) {
        log.error(err);

        try {
            ses.send(encode(new OdbcResponse(OdbcResponse.STATUS_FAILED, err)));
        }
        catch (Exception e) {
            log.error("Can not send error response message: [err=" + e.getMessage() + ']');
        }
    }

    /**
     * Decode OdbcRequest from byte array.
     *
     * @param msg Message.
     * @return Assembled ODBC request.
     */
    private OdbcRequest decode(byte[] msg) throws IOException {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null);

        OdbcRequest res;

        byte cmd = reader.readByte();

        switch (cmd) {
            case OdbcRequest.EXECUTE_SQL_QUERY: {

                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                if (log.isDebugEnabled()) {
                    log.debug("Message: [cmd=EXECUTE_SQL_QUERY, cache=" + cache +
                            ", query=" + sql + ", argsNum=" + argsNum + ']');
                }

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = reader.readObjectDetached();

                res = new OdbcQueryExecuteRequest(cache, sql, params);

                break;
            }

            case OdbcRequest.FETCH_SQL_QUERY: {

                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                if (log.isDebugEnabled())
                    log.debug("Message: [cmd=FETCH_SQL_QUERY, queryId=" + queryId + ", pageSize=" + pageSize + ']');

                res = new OdbcQueryFetchRequest(queryId, pageSize);

                break;
            }

            case OdbcRequest.CLOSE_SQL_QUERY: {

                long queryId = reader.readLong();

                if (log.isDebugEnabled()) {
                    log.debug("Message: [cmd=CLOSE_SQL_QUERY, queryId=" + queryId + ']');
                }

                res = new OdbcQueryCloseRequest(queryId);

                break;
            }

            case OdbcRequest.GET_COLUMNS_META: {

                String cache = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                if (log.isDebugEnabled()) {
                    log.debug("Message: [cmd=GET_COLUMNS_META, cache=" + cache +
                            ", table=" + table + ", column: " + column + ']');
                }

                res = new OdbcQueryGetColumnsMetaRequest(cache, table, column);

                break;
            }

            case OdbcRequest.GET_TABLES_META: {

                String catalog = reader.readString();
                String schema = reader.readString();
                String table = reader.readString();
                String tableType = reader.readString();

                if (log.isDebugEnabled()) {
                    log.debug("Message: [cmd=GET_COLUMNS_META, catalog=" + catalog +
                            ", schema=" + schema + ", table=" + table + ", tableType=" + tableType + ']');
                }

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            default:
                throw new IOException("Failed to parse incoming packet (unknown command type) " +
                        "[cmd=[" + Byte.toString(cmd) + ']');
        }

        return res;
    }

    /**
     * Encode OdbcResponse to byte array.
     *
     * @param msg Message.
     * @return Byte array.
     */
    private byte[] encode(OdbcResponse msg) throws IOException {
        assert msg != null;

        // Creating new binary writer
        BinaryRawWriterEx writer = marsh.writer(new BinaryHeapOutputStream(INIT_CAP));

        // Writing status
        writer.writeByte((byte) msg.status());

        if (msg.status() != OdbcResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return Arrays.copyOf(writer.out().array(), writer.out().position());
        }

        Object res0 = msg.response();

        if (res0 instanceof OdbcQueryExecuteResult) {
            OdbcQueryExecuteResult res = (OdbcQueryExecuteResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());

            Collection<OdbcColumnMeta> metas = res.getColumnsMetadata();

            assert metas != null;

            writer.writeInt(metas.size());

            for (OdbcColumnMeta meta : metas)
                meta.writeBinary(writer, marsh.context());

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
                        writer.writeObjectDetached(obj);
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
                columnMeta.writeBinary(writer, marsh.context());

        }
        else if (res0 instanceof OdbcQueryGetTablesMetaResult) {
            OdbcQueryGetTablesMetaResult res = (OdbcQueryGetTablesMetaResult) res0;

            Collection<OdbcTableMeta> tablesMeta = res.meta();

            assert tablesMeta != null;

            writer.writeInt(tablesMeta.size());

            for (OdbcTableMeta tableMeta : tablesMeta)
                tableMeta.writeBinary(writer);
        }
        else
            throw new IOException("Failed to serialize response packet (unknown response type)");

        return Arrays.copyOf(writer.out().array(), writer.out().position());
    }

    /**
     * Handle request.
     *
     * @param req Request.
     * @return Response.
     */
    private OdbcResponse handle(OdbcRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Received request from client: [req=" + req + ']');

        if (!busyLock.enterBusy()) {
            String errMsg = "Failed to handle request [req=" + req +
                    ", err=Received request while stopping grid]";

            U.error(log, errMsg);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, errMsg);
        }

        try {
            return handler.handle(req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }
}
