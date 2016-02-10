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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.*;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * ODBC protocol parser.
 */
public class OdbcParser implements GridNioParser {
    /** Initial output stream capacity. */
    private static final int INIT_CAP = 1024;

    /** Length in bytes of the remaining message part. */
    private int leftToReceive = 0;

    /** Already received bytes of current message. */
    private ByteBuffer currentMessage = null;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * @param ctx Kernel context.
     */
    public OdbcParser(GridKernalContext ctx) {
        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

        marsh = cacheObjProc.marshaller();

        log = ctx.log(getClass());
    }

    /**
     * Process data chunk and try to construct new message using stored and
     * freshly received data.
     *
     * @param buf Fresh data buffer.
     * @return Instance of the {@link BinaryReaderExImpl} positioned to read
     *     from the beginning of the message on success and null otherwise.
     */
    private BinaryRawReaderEx tryConstructMessage(ByteBuffer buf) {
        if (leftToReceive != 0) {
            // Still receiving message
            int toConsume = Math.min(leftToReceive, buf.remaining());

            currentMessage.put(buf.array(), buf.arrayOffset(), toConsume);
            leftToReceive -= toConsume;

            buf.position(buf.position() + toConsume);

            if (leftToReceive != 0)
                return null;

            BinaryInputStream stream = new BinaryHeapInputStream(currentMessage.array());

            BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null);

            currentMessage = null;

            return reader;
        }

        // Receiving new message
        BinaryInputStream stream = new BinaryHeapInputStream(buf.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, stream, null);

        // Getting message length. It's in the first four bytes of the message.
        int messageLen = reader.readInt();

        // Just skipping int here to sync position.
        buf.getInt();

        int remaining = buf.remaining();

        // Checking if we have not entire message in buffer.
        if (messageLen > remaining) {
            leftToReceive = messageLen - remaining;

            currentMessage = ByteBuffer.allocate(messageLen);
            currentMessage.put(buf);

            return null;
        }

        buf.position(buf.position() + messageLen);

        return reader;
    }

    /** {@inheritDoc} */
    @Nullable @Override public OdbcRequest decode(GridNioSession ses, ByteBuffer buf) throws IOException,
            IgniteCheckedException {
        BinaryRawReaderEx messageReader = tryConstructMessage(buf);

        return messageReader == null ? null : readRequest(ses, messageReader);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        assert msg != null;
        assert msg instanceof OdbcResponse;

        if (log.isDebugEnabled())
            log.debug("Encoding query processing result");

        BinaryRawWriterEx writer = marsh.writer(new BinaryHeapOutputStream(INIT_CAP));

        // Reserving space for the message length.
        int msgLenPos = writer.reserveInt();

        writeResponse(ses, writer, (OdbcResponse)msg);

        int msgLenWithHdr = writer.out().position() - msgLenPos;

        int msgLen = msgLenWithHdr - 4;

        writer.writeInt(msgLenPos, msgLen);

        ByteBuffer buf = ByteBuffer.allocate(msgLenWithHdr);

        buf.put(writer.out().array(), msgLenPos, msgLenWithHdr);

        buf.flip();

        return buf;
    }

    /**
     * Read ODBC request from the raw data using provided {@link BinaryReaderExImpl} instance.
     *
     * @param ses Current session.
     * @param reader Reader positioned to read the request.
     * @return Instance of the {@link OdbcRequest}.
     * @throws IOException if the type of the request is unknown to the parser.
     */
    private OdbcRequest readRequest(GridNioSession ses, BinaryRawReaderEx reader) throws IOException {
        OdbcRequest res;

        byte cmd = reader.readByte();

        switch (cmd) {
            case OdbcRequest.EXECUTE_SQL_QUERY: {

                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                if (log.isDebugEnabled()) {
                    log.debug("Message EXECUTE_SQL_QUERY:");
                    log.debug("cache: " + cache);
                    log.debug("query: " + sql);
                    log.debug("argsNum: " + argsNum);
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

                if (log.isDebugEnabled()) {
                    log.debug("Message FETCH_SQL_QUERY:");
                    log.debug("queryId: " + queryId);
                    log.debug("pageSize: " + pageSize);
                }

                res = new OdbcQueryFetchRequest(queryId, pageSize);

                break;
            }

            case OdbcRequest.CLOSE_SQL_QUERY: {

                long queryId = reader.readLong();

                if (log.isDebugEnabled()) {
                    log.debug("Message CLOSE_SQL_QUERY:");
                    log.debug("queryId: " + queryId);
                }

                res = new OdbcQueryCloseRequest(queryId);

                break;
            }

            case OdbcRequest.GET_COLUMNS_META: {

                String cache = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                if (log.isDebugEnabled()) {
                    log.debug("Message GET_COLUMNS_META:");
                    log.debug("cache: " + cache);
                    log.debug("table: " + table);
                    log.debug("column: " + column);
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
                    log.debug("Message GET_COLUMNS_META:");
                    log.debug("catalog: " + catalog);
                    log.debug("schema: " + schema);
                    log.debug("table: " + table);
                    log.debug("tableType: " + tableType);
                }

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            default:
                throw new IOException("Failed to parse incoming packet (unknown command type) [ses=" + ses +
                        ", cmd=[" + Byte.toString(cmd) + ']');
        }

        return res;
    }

    /**
     * Write ODBC response using provided {@link BinaryRawWriterEx} instance.
     *
     * @param ses Current session.
     * @param writer Writer.
     * @param rsp ODBC response that should be written.
     * @throws IOException if the type of the response is unknown to the parser.
     */
    private void writeResponse(GridNioSession ses, BinaryRawWriterEx writer, OdbcResponse rsp) throws IOException {
        // Writing status
        writer.writeByte((byte)rsp.status());

        if (rsp.status() != OdbcResponse.STATUS_SUCCESS) {
            writer.writeString(rsp.error());

            return;
        }

        Object res0 = rsp.response();

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
            throw new IOException("Failed to serialize response packet (unknown response type) [ses=" + ses + "]");
    }
}
