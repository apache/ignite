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
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ODBC message listener.
 */
public class OdbcNioListener extends GridNioServerListenerAdapter<byte[]> {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

    /** Current ODBC communication protocol version */
    private static final short PROTOCOL_VERSION = 1;

    /** Handler metadata key. */
    private static final int HANDLER_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Request ID generator. */
    private static final AtomicLong REQ_ID_GEN = new AtomicLong();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     * @param busyLock Shutdown busy lock.
     */
    public OdbcNioListener(GridKernalContext ctx, GridSpinBusyLock busyLock) {
        this.ctx = ctx;
        this.busyLock = busyLock;

        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

        this.marsh = cacheObjProc.marshaller();

        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("ODBC client connected: " + ses.remoteAddress());

        ses.addMeta(HANDLER_META_KEY, new OdbcRequestHandler(ctx, busyLock));
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("ODBC client disconnected: " + ses.remoteAddress());
            else
                log.debug("ODBC client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, byte[] msg) {
        assert msg != null;

        long reqId = REQ_ID_GEN.incrementAndGet();

        try {
            long startTime = 0;

            OdbcRequest req = decode(msg);

            if (log.isDebugEnabled()) {
                startTime = System.nanoTime();

                log.debug("ODBC request received [id=" + reqId + ", addr=" + ses.remoteAddress() +
                    ", req=" + req + ']');
            }

            OdbcRequestHandler handler = ses.meta(HANDLER_META_KEY);

            assert handler != null;

            OdbcResponse resp = handler.handle(req);

            if (log.isDebugEnabled()) {
                long dur = (System.nanoTime() - startTime) / 1000;

                log.debug("ODBC request processed [id=" + reqId + ", dur(mcs)=" + dur  +
                    ", resp=" + resp.status() + ']');
            }

            byte[] outMsg = encode(resp);

            ses.send(outMsg);
        }
        catch (Exception e) {
            log.error("Failed to process ODBC request [id=" + reqId + ", err=" + e + ']');

            ses.send(encode(new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage())));
        }
    }

    /**
     * Decode OdbcRequest from byte array.
     *
     * @param msg Message.
     * @return Assembled ODBC request.
     */
    private OdbcRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null);

        OdbcRequest res;

        short version = reader.readShort();

        if (version != PROTOCOL_VERSION)
            throw new IgniteException("Unsupported ODBC communication protocol version: " +
                    "[ver=" + version + ", current_ver=" + PROTOCOL_VERSION + ']');

        byte cmd = reader.readByte();

        switch (cmd) {
            case OdbcRequest.EXECUTE_SQL_QUERY: {
                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = reader.readObjectDetached();

                res = new OdbcQueryExecuteRequest(cache, sql, params);

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

            default:
                throw new IgniteException("Unknown ODBC command: " + cmd);
        }

        return res;
    }

    /**
     * Encode OdbcResponse to byte array.
     *
     * @param msg Message.
     * @return Byte array.
     */
    private byte[] encode(OdbcResponse msg) {
        assert msg != null;

        // Creating new binary writer
        BinaryWriterExImpl writer = marsh.writer(new BinaryHeapOutputStream(INIT_CAP));

        // Writing protocol version.
        writer.writeShort(PROTOCOL_VERSION);

        // Writing status.
        writer.writeByte((byte) msg.status());

        if (msg.status() != OdbcResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return writer.array();
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
            assert false : "Should not reach here.";

        return writer.array();
    }
}
