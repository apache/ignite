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

import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.BulkLoadContextCursor;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinPartitionAwarenessMappingGroup;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponseSender;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.transactions.TransactionAlreadyCompletedException;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.apache.ignite.transactions.TransactionMixedModeException;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.apache.ignite.transactions.TransactionUnsupportedConcurrencyException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_CONTINUE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_EOF;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_ERROR;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_3_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_4_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_7_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_8_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_9_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BATCH_EXEC;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BATCH_EXEC_ORDERED;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BINARY_TYPE_GET;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BINARY_TYPE_NAME_GET;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BINARY_TYPE_NAME_PUT;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BINARY_TYPE_PUT;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BULK_LOAD_BATCH;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.CACHE_PARTITIONS;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_COLUMNS;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_INDEXES;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_PARAMS;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_PRIMARY_KEYS;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_SCHEMAS;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.META_TABLES;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_CANCEL;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_CLOSE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_EXEC;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_FETCH;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_META;

/**
 * JDBC request handler.
 */
public class JdbcRequestHandler implements ClientListenerRequestHandler {
    /** Jdbc query cancelled response. */
    private static final JdbcResponse JDBC_QUERY_CANCELLED_RESPONSE =
        new JdbcResponse(IgniteQueryErrorCode.QUERY_CANCELED, QueryCancelledException.ERR_MSG);

    /** JDBC connection context. */
    private final JdbcConnectionContext connCtx;

    /** Client context. */
    private final SqlClientContext cliCtx;

    /** Logger. */
    private final IgniteLogger log;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Worker. */
    private final JdbcRequestHandlerWorker worker;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Current JDBC cursors. */
    private final ConcurrentHashMap<Long, JdbcCursor> jdbcCursors = new ConcurrentHashMap<>();

    /** Ordered batches queue. */
    private final PriorityQueue<JdbcOrderedBatchExecuteRequest> orderedBatchesQueue = new PriorityQueue<>();

    /** Ordered batches mutex. */
    private final Object orderedBatchesMux = new Object();

    /** Request mutex. */
    private final Object reqMux = new Object();

    /** Response sender. */
    private final ClientListenerResponseSender sender;

    /** Automatic close of cursors. */
    private final boolean autoCloseCursors;

    /** Nested transactions handling mode. */
    private final NestedTxMode nestedTxMode;

    /** Protocol version. */
    private final ClientListenerProtocolVersion protocolVer;

    /** Authentication context */
    private AuthorizationContext actx;

    /** Facade that hides transformations internal cache api entities -> jdbc metadata. */
    private final JdbcMetadataInfo meta;

    /** Register that keeps non-cancelled requests. */
    private Map<Long, JdbcQueryDescriptor> reqRegister = new HashMap<>();

    /**
     * Constructor.
     * @param busyLock Shutdown latch.
     * @param sender Results sender.
     * @param maxCursors Maximum allowed cursors.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param replicatedOnly Replicated only flag.
     * @param autoCloseCursors Flag to automatically close server cursors.
     * @param lazy Lazy query execution flag.
     * @param skipReducerOnUpdate Skip reducer on update flag.
     * @param dataPageScanEnabled Enable scan data page mode.
     * @param updateBatchSize Size of internal batch for DML queries.
     * @param actx Authentication context.
     * @param protocolVer Protocol version.
     * @param connCtx Jdbc connection context.
     */
    public JdbcRequestHandler(
        GridSpinBusyLock busyLock,
        ClientListenerResponseSender sender,
        int maxCursors,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean collocated,
        boolean replicatedOnly,
        boolean autoCloseCursors,
        boolean lazy,
        boolean skipReducerOnUpdate,
        NestedTxMode nestedTxMode,
        @Nullable Boolean dataPageScanEnabled,
        @Nullable Integer updateBatchSize,
        AuthorizationContext actx,
        ClientListenerProtocolVersion protocolVer,
        JdbcConnectionContext connCtx
    ) {
        this.connCtx = connCtx;
        this.sender = sender;

        meta = new JdbcMetadataInfo(connCtx.kernalContext());

        Factory<GridWorker> orderedFactory = new Factory<GridWorker>() {
            @Override public GridWorker create() {
                return new OrderedBatchWorker();
            }
        };

        cliCtx = new SqlClientContext(
            connCtx.kernalContext(),
            orderedFactory,
            distributedJoins,
            enforceJoinOrder,
            collocated,
            replicatedOnly,
            lazy,
            skipReducerOnUpdate,
            dataPageScanEnabled,
            updateBatchSize
        );

        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.autoCloseCursors = autoCloseCursors;
        this.nestedTxMode = nestedTxMode;
        this.protocolVer = protocolVer;
        this.actx = actx;

        log = connCtx.kernalContext().log(getClass());

        // TODO IGNITE-9484 Do not create worker if there is a possibility to unbind TX from threads.
        worker = new JdbcRequestHandlerWorker(connCtx.kernalContext().igniteInstanceName(), log, this,
            connCtx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handle(ClientListenerRequest req0) {
        assert req0 != null;

        assert req0 instanceof JdbcRequest;

        JdbcRequest req = (JdbcRequest)req0;

        if (!MvccUtils.mvccEnabled(connCtx.kernalContext()))
            return doHandle(req);
        else {
            GridFutureAdapter<ClientListenerResponse> fut = worker.process(req);

            try {
                return fut.get();
            }
            catch (IgniteCheckedException e) {
                return exceptionToResult(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancellationCommand(int cmdId) {
        return cmdId == JdbcRequest.QRY_CANCEL;
    }

    /** {@inheritDoc} */
    @Override public void registerRequest(long reqId, int cmdType) {
        assert reqId != 0;

        synchronized (reqMux) {
            if (isCancellationSupported() && (cmdType == QRY_EXEC || cmdType == BATCH_EXEC))
                reqRegister.put(reqId, new JdbcQueryDescriptor());
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterRequest(long reqId) {
        assert reqId != 0;

        synchronized (reqMux) {
            if (isCancellationSupported())
                reqRegister.remove(reqId);
        }
    }

    /**
     * Start worker, if it's present.
     */
    void start() {
        if (worker != null)
            worker.start();
    }

    /**
     * Actually handle the request.
     * @param req Request.
     * @return Request handling result.
     */
    JdbcResponse doHandle(JdbcRequest req) {
        if (!busyLock.enterBusy())
            return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                "Failed to handle JDBC request because node is stopping.");

        if (actx != null)
            AuthorizationContext.context(actx);

        JdbcResponse resp;
        try {
            switch (req.type()) {
                case QRY_EXEC:
                    resp = executeQuery((JdbcQueryExecuteRequest)req);
                    break;

                case QRY_FETCH:
                    resp = fetchQuery((JdbcQueryFetchRequest)req);
                    break;

                case QRY_CLOSE:
                    resp = closeQuery((JdbcQueryCloseRequest)req);
                    break;

                case QRY_META:
                    resp = getQueryMeta((JdbcQueryMetadataRequest)req);
                    break;

                case BATCH_EXEC:
                    resp = executeBatch((JdbcBatchExecuteRequest)req);
                    break;

                case BATCH_EXEC_ORDERED:
                    resp = dispatchBatchOrdered((JdbcOrderedBatchExecuteRequest)req);
                    break;

                case META_TABLES:
                    resp = getTablesMeta((JdbcMetaTablesRequest)req);
                    break;

                case META_COLUMNS:
                    resp = getColumnsMeta((JdbcMetaColumnsRequest)req);
                    break;

                case META_INDEXES:
                    resp = getIndexesMeta((JdbcMetaIndexesRequest)req);
                    break;

                case META_PARAMS:
                    resp = getParametersMeta((JdbcMetaParamsRequest)req);
                    break;

                case META_PRIMARY_KEYS:
                    resp = getPrimaryKeys((JdbcMetaPrimaryKeysRequest)req);
                    break;

                case META_SCHEMAS:
                    resp = getSchemas((JdbcMetaSchemasRequest)req);
                    break;

                case BULK_LOAD_BATCH:
                    resp = processBulkLoadFileBatch((JdbcBulkLoadBatchRequest)req);
                    break;

                case QRY_CANCEL:
                    resp = cancelQuery((JdbcQueryCancelRequest)req);
                    break;

                case CACHE_PARTITIONS:
                    resp = getCachePartitions((JdbcCachePartitionsRequest)req);
                    break;

                case BINARY_TYPE_NAME_PUT:
                    resp = registerBinaryType((JdbcBinaryTypeNamePutRequest)req);
                    break;

                case BINARY_TYPE_NAME_GET:
                    resp = getBinaryTypeName((JdbcBinaryTypeNameGetRequest)req);
                    break;

                case BINARY_TYPE_PUT:
                    resp = putBinaryType((JdbcBinaryTypePutRequest)req);
                    break;

                case BINARY_TYPE_GET:
                    resp = getBinaryType((JdbcBinaryTypeGetRequest)req);
                    break;

                default:
                    resp = new JdbcResponse(IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported JDBC request [req=" + req + ']');
            }

            if (resp != null)
                resp.activeTransaction(connCtx.kernalContext().cache().context().tm().inUserTx());

            return resp;
        }
        finally {
            AuthorizationContext.clear();

            busyLock.leaveBusy();
        }
    }

    /**
     * @param req Ordered batch request.
     * @return Response.
     */
    private JdbcResponse dispatchBatchOrdered(JdbcOrderedBatchExecuteRequest req) {
        if (!cliCtx.isStreamOrdered())
            executeBatchOrdered(req);
        else {
            synchronized (orderedBatchesMux) {
                orderedBatchesQueue.add(req);

                orderedBatchesMux.notifyAll();
            }
        }

       return null;
    }

    /**
     * @param req Ordered batch request.
     * @return Response.
     */
    private ClientListenerResponse executeBatchOrdered(JdbcOrderedBatchExecuteRequest req) {
        try {
            if (req.isLastStreamBatch())
                cliCtx.waitTotalProcessedOrderedRequests(req.order());

            JdbcResponse resp = executeBatch(req);

            if (resp.response() instanceof JdbcBatchExecuteResult) {
                resp = new JdbcResponse(
                    new JdbcOrderedBatchExecuteResult((JdbcBatchExecuteResult)resp.response(), req.order()));
            }

            sender.send(resp);
        } catch (Exception e) {
            U.error(null, "Error processing file batch", e);

            sender.send(new JdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Server error: " + e));
        }

        cliCtx.orderedRequestProcessed();

        return null;
    }

    /**
     * Processes a file batch sent from client as part of bulk load COPY command.
     *
     * @param req Request object with a batch of a file received from client.
     * @return Response to send to the client.
     */
    private JdbcResponse processBulkLoadFileBatch(JdbcBulkLoadBatchRequest req) {
        if (connCtx.kernalContext() == null)
            return new JdbcResponse(IgniteQueryErrorCode.UNEXPECTED_OPERATION, "Unknown query ID: "
                + req.cursorId() + ". Bulk load session may have been reclaimed due to timeout.");

        JdbcBulkLoadProcessor processor = (JdbcBulkLoadProcessor)jdbcCursors.get(req.cursorId());

        if (!prepareQueryCancellationMeta(processor))
            return JDBC_QUERY_CANCELLED_RESPONSE;

        boolean unregisterReq = false;

        try {
            processor.processBatch(req);

            switch (req.cmd()) {
                case CMD_FINISHED_ERROR:
                case CMD_FINISHED_EOF:
                    jdbcCursors.remove(req.cursorId());

                    processor.close();

                    unregisterReq = true;

                    break;

                case CMD_CONTINUE:
                    break;

                default:
                    throw new IllegalArgumentException();
            }

            return resultToResonse(new JdbcQueryExecuteResult(req.cursorId(), processor.updateCnt(), null));
        }
        catch (Exception e) {
            U.error(null, "Error processing file batch", e);

            processor.onFail(e);

            if (X.cause(e, QueryCancelledException.class) != null)
                return exceptionToResult(new QueryCancelledException());
            else
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Server error: " + e);
        }
        finally {
            cleanupQueryCancellationMeta(unregisterReq, processor.requestId());
        }
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handleException(Throwable e, ClientListenerRequest req) {
        return exceptionToResult(e);
    }

    /** {@inheritDoc} */
    @Override public void writeHandshake(BinaryWriterExImpl writer) {
        // Handshake OK.
        writer.writeBoolean(true);

        // Write server version.
        writer.writeByte(IgniteVersionUtils.VER.major());
        writer.writeByte(IgniteVersionUtils.VER.minor());
        writer.writeByte(IgniteVersionUtils.VER.maintenance());
        writer.writeString(IgniteVersionUtils.VER.stage());
        writer.writeLong(IgniteVersionUtils.VER.revisionTimestamp());
        writer.writeByteArray(IgniteVersionUtils.VER.revisionHash());

        // Write node id.
        if (protocolVer.compareTo(VER_2_8_0) >= 0)
            writer.writeUuid(connCtx.kernalContext().localNodeId());

        // Write all features supported by the node.
        if (protocolVer.compareTo(VER_2_9_0) >= 0)
            writer.writeByteArray(ThinProtocolFeature.featuresAsBytes(connCtx.protocolContext().features()));
    }

    /**
     * Called whenever client is disconnected due to correct connection close
     * or due to {@code IOException} during network operations.
     */
    public void onDisconnect() {
        if (worker != null) {
            worker.cancel();

            try {
                worker.join();
            }
            catch (InterruptedException e) {
                // No-op.
            }
        }

        for (JdbcCursor cursor : jdbcCursors.values())
            U.close(cursor, log);

        jdbcCursors.clear();

        synchronized (reqMux) {
            reqRegister.clear();
        }

        U.close(cliCtx, log);
    }

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    @SuppressWarnings("unchecked")
    private JdbcResponse executeQuery(JdbcQueryExecuteRequest req) {
        GridQueryCancel cancel = null;

        boolean unregisterReq = false;

        if (isCancellationSupported()) {
            synchronized (reqMux) {
                JdbcQueryDescriptor desc = reqRegister.get(req.requestId());

                // Query was already cancelled and unregistered.
                if (desc == null)
                    return null;

                cancel = desc.cancelHook();

                desc.incrementUsageCount();
            }
        }

        try {
            int cursorCnt = jdbcCursors.size();

            if (maxCursors > 0 && cursorCnt >= maxCursors)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Too many open cursors (either close other " +
                    "open cursors or increase the limit through " +
                    "ClientConnectorConfiguration.maxOpenCursorsPerConnection) [maximum=" + maxCursors +
                    ", current=" + cursorCnt + ']');

            assert !cliCtx.isStream();

            String sql = req.sqlQuery();

            SqlFieldsQueryEx qry;

            switch (req.expectedStatementType()) {
                case ANY_STATEMENT_TYPE:
                    qry = new SqlFieldsQueryEx(sql, null);

                    break;

                case SELECT_STATEMENT_TYPE:
                    qry = new SqlFieldsQueryEx(sql, true);

                    break;

                default:
                    assert req.expectedStatementType() == JdbcStatementType.UPDATE_STMT_TYPE;

                    qry = new SqlFieldsQueryEx(sql, false);

                    if (cliCtx.isSkipReducerOnUpdate())
                        ((SqlFieldsQueryEx)qry).setSkipReducerOnUpdate(true);
            }

            setupQuery(qry, prepareSchemaName(req.schemaName()));

            qry.setArgs(req.arguments());
            qry.setAutoCommit(req.autoCommit());

            if (req.pageSize() <= 0)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Invalid fetch size: " + req.pageSize());

            qry.setPageSize(req.pageSize());

            String schemaName = req.schemaName();

            if (F.isEmpty(schemaName))
                schemaName = QueryUtils.DFLT_SCHEMA;

            qry.setSchema(schemaName);

            List<FieldsQueryCursor<List<?>>> results = connCtx.kernalContext().query().querySqlFields(null, qry,
                cliCtx, true, protocolVer.compareTo(VER_2_3_0) < 0, cancel);

            FieldsQueryCursor<List<?>> fieldsCur = results.get(0);

            if (fieldsCur instanceof BulkLoadContextCursor) {
                BulkLoadContextCursor blCur = (BulkLoadContextCursor)fieldsCur;

                BulkLoadProcessor blProcessor = blCur.bulkLoadProcessor();
                BulkLoadAckClientParameters clientParams = blCur.clientParams();

                JdbcBulkLoadProcessor processor = new JdbcBulkLoadProcessor(blProcessor, req.requestId());

                jdbcCursors.put(processor.cursorId(), processor);

                // responses for the same query on the client side
                return resultToResonse(new JdbcBulkLoadAckResult(processor.cursorId(), clientParams));
            }

            if (results.size() == 1) {
                JdbcQueryCursor cur = new JdbcQueryCursor(req.pageSize(), req.maxRows(),
                    (QueryCursorImpl)fieldsCur, req.requestId());

                jdbcCursors.put(cur.cursorId(), cur);

                cur.openIterator();

                JdbcQueryExecuteResult res;

                PartitionResult partRes = ((QueryCursorImpl<List<?>>)fieldsCur).partitionResult();

                if (cur.isQuery())
                    res = new JdbcQueryExecuteResult(cur.cursorId(), cur.fetchRows(), !cur.hasNext(),
                        isClientPartitionAwarenessApplicable(req.partitionResponseRequest(), partRes) ?
                            partRes :
                            null);
                else {
                    List<List<Object>> items = cur.fetchRows();

                    assert items != null && items.size() == 1 && items.get(0).size() == 1
                        && items.get(0).get(0) instanceof Long :
                        "Invalid result set for not-SELECT query. [qry=" + sql +
                            ", res=" + S.toString(List.class, items) + ']';

                    res = new JdbcQueryExecuteResult(cur.cursorId(), (Long)items.get(0).get(0),
                        isClientPartitionAwarenessApplicable(req.partitionResponseRequest(), partRes) ?
                            partRes :
                            null);
                }

                if (res.last() && (!res.isQuery() || autoCloseCursors)) {
                    jdbcCursors.remove(cur.cursorId());

                    unregisterReq = true;

                    cur.close();
                }

                return resultToResonse(res);
            }
            else {
                List<JdbcResultInfo> jdbcResults = new ArrayList<>(results.size());
                List<List<Object>> items = null;
                boolean last = true;

                for (FieldsQueryCursor<List<?>> c : results) {
                    QueryCursorImpl qryCur = (QueryCursorImpl)c;

                    JdbcResultInfo jdbcRes;

                    if (qryCur.isQuery()) {
                        JdbcQueryCursor cur = new JdbcQueryCursor(req.pageSize(), req.maxRows(), qryCur, req.requestId());

                        jdbcCursors.put(cur.cursorId(), cur);

                        jdbcRes = new JdbcResultInfo(true, -1, cur.cursorId());

                        cur.openIterator();

                        if (items == null) {
                            items = cur.fetchRows();
                            last = cur.hasNext();
                        }
                    }
                    else
                        jdbcRes = new JdbcResultInfo(false, (Long)((List<?>)qryCur.getAll().get(0)).get(0), -1);

                    jdbcResults.add(jdbcRes);
                }

                return resultToResonse(new JdbcQueryExecuteMultipleStatementsResult(jdbcResults, items, last));
            }
        }
        catch (Exception e) {
            // Trying to close all cursors of current request.
            clearCursors(req.requestId());

            unregisterReq = true;

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            if (X.cause(e, QueryCancelledException.class) != null)
                return exceptionToResult(new QueryCancelledException());
            else
                return exceptionToResult(e);
        }
        finally {
            cleanupQueryCancellationMeta(unregisterReq, req.requestId());
        }
    }

    /**
     * {@link JdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse closeQuery(JdbcQueryCloseRequest req) {
        JdbcCursor cur = jdbcCursors.get(req.cursorId());

        if (!prepareQueryCancellationMeta(cur))
            return new JdbcResponse(null);

        try {
            cur = jdbcCursors.remove(req.cursorId());

            if (cur == null)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to find query cursor with ID: " + req.cursorId());

            cur.close();

            return new JdbcResponse(null);
        }
        catch (Exception e) {
            jdbcCursors.remove(req.cursorId());

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            if (X.cause(e, QueryCancelledException.class) != null)
                return new JdbcResponse(null);
            else
                return exceptionToResult(e);
        }
        finally {
            if (isCancellationSupported()) {
                boolean clearCursors = false;

                synchronized (reqMux) {
                    assert cur != null;

                    JdbcQueryDescriptor desc = reqRegister.get(cur.requestId());

                    if (desc != null) {
                        // Query was cancelled during execution.
                        if (desc.isCanceled()) {
                            clearCursors = true;

                            unregisterRequest(req.requestId());
                        }
                        else {
                            tryUnregisterRequest(cur.requestId());

                            desc.decrementUsageCount();
                        }
                    }
                }

                if (clearCursors)
                    clearCursors(cur.requestId());
            }
        }
    }

    /**
     * {@link JdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse fetchQuery(JdbcQueryFetchRequest req) {
        final JdbcQueryCursor cur = (JdbcQueryCursor)jdbcCursors.get(req.cursorId());

        if (!prepareQueryCancellationMeta(cur))
            return JDBC_QUERY_CANCELLED_RESPONSE;

        boolean unregisterReq = false;

        try {
            if (cur == null)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to find query cursor with ID: " + req.cursorId());

            if (req.pageSize() <= 0)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

            cur.pageSize(req.pageSize());

            JdbcQueryFetchResult res = new JdbcQueryFetchResult(cur.fetchRows(), !cur.hasNext());

            if (res.last() && (!cur.isQuery() || autoCloseCursors)) {
                jdbcCursors.remove(req.cursorId());

                unregisterReq = true;

                cur.close();
            }

            return resultToResonse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            if (X.cause(e, QueryCancelledException.class) != null)
                return exceptionToResult(new QueryCancelledException());
            else
                return exceptionToResult(e);
        }
        finally {
            assert cur != null;

            cleanupQueryCancellationMeta(unregisterReq, cur.requestId());
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getQueryMeta(JdbcQueryMetadataRequest req) {
        final JdbcQueryCursor cur = (JdbcQueryCursor)jdbcCursors.get(req.cursorId());

        if (!prepareQueryCancellationMeta(cur))
            return JDBC_QUERY_CANCELLED_RESPONSE;

        try {
            if (cur == null)
                return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to find query cursor with ID: " + req.cursorId());

            JdbcQueryMetadataResult res = new JdbcQueryMetadataResult(req.cursorId(),
                cur.meta());

            return resultToResonse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
        finally {
            assert cur != null;

            cleanupQueryCancellationMeta(false, cur.requestId());
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse executeBatch(JdbcBatchExecuteRequest req) {
        GridQueryCancel cancel = null;

        // Skip request register check for ORDERED batches (JDBC streams)
        // because ordered batch requests are processed asynchronously at the
        // separate thread.
        if (isCancellationSupported() && req.type() == BATCH_EXEC) {
            synchronized (reqMux) {
                JdbcQueryDescriptor desc = reqRegister.get(req.requestId());

                // Query was already cancelled and unregisterd.
                if (desc == null)
                    return null;

                cancel = desc.cancelHook();

                desc.incrementUsageCount();
            }
        }

        try {
            String schemaName = prepareSchemaName(req.schemaName());

            int qryCnt = req.queries().size();

            List<Integer> updCntsAcc = new ArrayList<>(qryCnt);

            // Send back only the first error. Others will be written to the log.
            IgniteBiTuple<Integer, String> firstErr = new IgniteBiTuple<>();

            SqlFieldsQueryEx qry = null;

            for (JdbcQuery q : req.queries()) {
                if (q.sql() != null) { // If we have a new query string in the batch,
                    if (qry != null) // then execute the previous sub-batch and create a new SqlFieldsQueryEx.
                        executeBatchedQuery(qry, updCntsAcc, firstErr, cancel);

                    qry = new SqlFieldsQueryEx(q.sql(), false);

                    setupQuery(qry, schemaName);

                    qry.setAutoCommit(req.autoCommit());
                }

                assert qry != null;

                qry.addBatchedArgs(q.args());
            }

            if (qry != null)
                executeBatchedQuery(qry, updCntsAcc, firstErr, cancel);

            if (req.isLastStreamBatch())
                cliCtx.disableStreaming();

            int updCnts[] = U.toIntArray(updCntsAcc);

            return firstErr.isEmpty() ?
                resultToResonse(
                    new JdbcBatchExecuteResult(updCnts, ClientListenerResponse.STATUS_SUCCESS, null)) :
                resultToResonse(new JdbcBatchExecuteResult(updCnts, firstErr.getKey(), firstErr.getValue()));
        }
        catch (QueryCancelledException e) {
            return exceptionToResult(e);
        }
        finally {
            cleanupQueryCancellationMeta(true, req.requestId());
        }
    }

    /**
     * Normalize schema name.
     *
     * @param schemaName Schema name.
     * @return Normalized schema name.
     */
    private static String prepareSchemaName(@Nullable String schemaName) {
        if (F.isEmpty(schemaName))
            schemaName = QueryUtils.DFLT_SCHEMA;

        return schemaName;
    }

    /**
     * Sets up query object with settings from current client context state and handler state.
     *
     * @param qry Query to setup.
     * @param schemaName Schema name.
     */
    private void setupQuery(SqlFieldsQueryEx qry, String schemaName) {
        qry.setDistributedJoins(cliCtx.isDistributedJoins());
        qry.setEnforceJoinOrder(cliCtx.isEnforceJoinOrder());
        qry.setCollocated(cliCtx.isCollocated());
        qry.setReplicatedOnly(cliCtx.isReplicatedOnly());
        qry.setLazy(cliCtx.isLazy());
        qry.setNestedTxMode(nestedTxMode);
        qry.setSchema(schemaName);
        qry.setTimeout(0, TimeUnit.MILLISECONDS);

        if (cliCtx.updateBatchSize() != null)
            qry.setUpdateBatchSize(cliCtx.updateBatchSize());
    }

    /**
     * Executes query and updates result counters.
     *
     * @param qry Query.
     * @param updCntsAcc Per query rows updates counter.
     * @param firstErr First error data - code and message.
     * @param cancel Hook for query cancellation.
     * @throws QueryCancelledException If query was cancelled during execution.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach"})
    private void executeBatchedQuery(SqlFieldsQueryEx qry, List<Integer> updCntsAcc,
        IgniteBiTuple<Integer, String> firstErr, GridQueryCancel cancel) throws QueryCancelledException {
        try {
            if (cliCtx.isStream()) {
                List<Long> cnt = connCtx.kernalContext().query().streamBatchedUpdateQuery(
                    qry.getSchema(),
                    cliCtx,
                    qry.getSql(),
                    qry.batchedArguments()
                );

                for (int i = 0; i < cnt.size(); i++)
                    updCntsAcc.add(cnt.get(i).intValue());

                return;
            }

            List<FieldsQueryCursor<List<?>>> qryRes = connCtx.kernalContext().query().querySqlFields(
                null, qry, cliCtx, true, true, cancel);

            for (FieldsQueryCursor<List<?>> cur : qryRes) {
                if (cur instanceof BulkLoadContextCursor)
                    throw new IgniteSQLException("COPY command cannot be executed in batch mode.");

                assert !((QueryCursorImpl)cur).isQuery();

                Iterator<List<?>> it = cur.iterator();

                if (it.hasNext()) {
                    int val = ((Long)it.next().get(0)).intValue();

                    updCntsAcc.add(val);
                }
            }
        }
        catch (Exception e) {
            int code;

            String msg;

            if (X.cause(e, QueryCancelledException.class) != null)
                throw new QueryCancelledException();
            else if (e instanceof IgniteSQLException) {
                BatchUpdateException batchCause = X.cause(e, BatchUpdateException.class);

                if (batchCause != null) {
                    int[] updCntsOnErr = batchCause.getUpdateCounts();

                    for (int i = 0; i < updCntsOnErr.length; i++)
                        updCntsAcc.add(updCntsOnErr[i]);

                    msg = batchCause.getMessage();

                    code = batchCause.getErrorCode();
                }
                else {
                    for (int i = 0; i < qry.batchedArguments().size(); i++)
                        updCntsAcc.add(Statement.EXECUTE_FAILED);

                    msg = e.getMessage();

                    code = ((IgniteSQLException)e).statusCode();
                }
            }
            else {
                for (int i = 0; i < qry.batchedArguments().size(); i++)
                    updCntsAcc.add(Statement.EXECUTE_FAILED);

                msg = e.getMessage();

                code = IgniteQueryErrorCode.UNKNOWN;
            }

            if (firstErr.isEmpty())
                firstErr.set(code, msg);
            else
                U.error(log, "Failed to execute batch query [qry=" + qry + ']', e);
        }
    }

    /**
     * @param req Get tables metadata request.
     * @return Response.
     */
    private JdbcResponse getTablesMeta(JdbcMetaTablesRequest req) {
        try {
            List<JdbcTableMeta> tabMetas = meta.getTablesMeta(req.schemaName(), req.tableName(), req.tableTypes());

            JdbcMetaTablesResult res = new JdbcMetaTablesResult(tabMetas);

            return resultToResonse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get tables metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * @param req Get columns metadata request.
     * @return Response.
     */
    private JdbcResponse getColumnsMeta(JdbcMetaColumnsRequest req) {
        try {
            Collection<JdbcColumnMeta> colsMeta =
                meta.getColumnsMeta(protocolVer, req.schemaName(), req.tableName(), req.columnName());

            JdbcMetaColumnsResult res;

            if (protocolVer.compareTo(VER_2_7_0) >= 0)
                res = new JdbcMetaColumnsResultV4(colsMeta);
            else if (protocolVer.compareTo(VER_2_4_0) >= 0)
                res = new JdbcMetaColumnsResultV3(colsMeta);
            else if (protocolVer.compareTo(VER_2_3_0) >= 0)
                res = new JdbcMetaColumnsResultV2(colsMeta);
            else
                res = new JdbcMetaColumnsResult(colsMeta);

            return resultToResonse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get columns metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Handler for updating binary type requests.
     *
     * @param req Incoming request.
     * @return Acknowledgement in case of successful updating.
     */
    private JdbcResponse putBinaryType(JdbcBinaryTypePutRequest req) {
        try {
            getBinaryCtx().updateMetadata(req.meta().typeId(), req.meta(), false);

            return resultToResonse(new JdbcUpdateBinarySchemaResult(req.requestId(), true));
        }
        catch (Exception e) {
            U.error(log, "Failed to update binary schema [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Handler for querying binary type requests.
     *
     * @param req Incoming request.
     * @return Response with binary type schema.
     */
    private JdbcResponse getBinaryType(JdbcBinaryTypeGetRequest req) {
        try {
            BinaryTypeImpl type = (BinaryTypeImpl)connCtx.kernalContext().cacheObjects().binary().type(req.typeId());

            return resultToResonse(new JdbcBinaryTypeGetResult(req.requestId(), type != null ? type.metadata() : null));
        }
        catch (Exception e) {
            U.error(log, "Failed to get binary type name [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Handler for querying binary type name requests.
     *
     * @param req Incoming request.
     * @return Response with binary type name.
     */
    private JdbcResponse getBinaryTypeName(JdbcBinaryTypeNameGetRequest req) {
        try {
            String name = getMarshallerCtx().getClassName(req.platformId(), req.typeId());

            return resultToResonse(new JdbcBinaryTypeNameGetResult(req.requestId(), name));
        }
        catch (Exception e) {
            U.error(log, "Failed to get binary type name [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Handler for register new binary type requests.
     *
     * @param req Incoming request.
     * @return Acknowledgement in case of successful registration.
     */
    private JdbcResponse registerBinaryType(JdbcBinaryTypeNamePutRequest req) {
        try {
            boolean res = getMarshallerCtx().registerClassName(req.platformId(), req.typeId(), req.typeName(), false);

            return resultToResonse(new JdbcUpdateBinarySchemaResult(req.requestId(), res));
        }
        catch (Exception e) {
            U.error(log, "Failed to register new type [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Get marshaller context from connection context.
     *
     * @return Marshaller context.
     */
    private MarshallerContext getMarshallerCtx() {
        return connCtx.kernalContext().marshallerContext();
    }

    /**
     * Get binary context from connection context.
     *
     * @return Binary context.
     */
    private BinaryContext getBinaryCtx() {
        return ((CacheObjectBinaryProcessorImpl)connCtx.kernalContext().cacheObjects()).binaryContext();
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getIndexesMeta(JdbcMetaIndexesRequest req) {
        try {
            Collection<JdbcIndexMeta> idxInfos = meta.getIndexesMeta(req.schemaName(), req.tableName());

            return resultToResonse(new JdbcMetaIndexesResult(idxInfos));
        }
        catch (Exception e) {
            U.error(log, "Failed to get parameters metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getParametersMeta(JdbcMetaParamsRequest req) {
        String schemaName = prepareSchemaName(req.schemaName());

        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(req.sql(), null);

        setupQuery(qry, schemaName);

        try {
            List<JdbcParameterMeta> meta = connCtx.kernalContext().query().getIndexing().
                parameterMetaData(schemaName, qry);

            JdbcMetaParamsResult res = new JdbcMetaParamsResult(meta);

            return resultToResonse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get parameters metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getPrimaryKeys(JdbcMetaPrimaryKeysRequest req) {
        try {
            Collection<JdbcPrimaryKeyMeta> pkMeta = meta.getPrimaryKeys(req.schemaName(), req.tableName());

            return resultToResonse(new JdbcMetaPrimaryKeysResult(pkMeta));
        }
        catch (Exception e) {
            U.error(log, "Failed to get parameters metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getSchemas(JdbcMetaSchemasRequest req) {
        try {
            String schemaPtrn = req.schemaName();

            SortedSet<String> schemas = meta.getSchemasMeta(schemaPtrn);

            return resultToResonse(new JdbcMetaSchemasResult(schemas));
        }
        catch (Exception e) {
            U.error(log, "Failed to get schemas metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Create {@link JdbcResponse} bearing appropriate Ignite specific result code if possible
     *     from given {@link Exception}.
     *
     * @param e Exception to convert.
     * @return resulting {@link JdbcResponse}.
     */
    private JdbcResponse exceptionToResult(Throwable e) {
        if (e instanceof QueryCancelledException)
            return new JdbcResponse(IgniteQueryErrorCode.QUERY_CANCELED, e.getMessage());
        if (e instanceof TransactionSerializationException)
            return new JdbcResponse(IgniteQueryErrorCode.TRANSACTION_SERIALIZATION_ERROR, e.getMessage());
        if (e instanceof TransactionAlreadyCompletedException)
            return new JdbcResponse(IgniteQueryErrorCode.TRANSACTION_COMPLETED, e.getMessage());
        if (e instanceof TransactionDuplicateKeyException)
            return new JdbcResponse(IgniteQueryErrorCode.DUPLICATE_KEY, e.getMessage());
        if (e instanceof TransactionMixedModeException)
            return new JdbcResponse(IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH, e.getMessage());
        if (e instanceof TransactionUnsupportedConcurrencyException)
            return new JdbcResponse(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, e.getMessage());
        if (e instanceof IgniteSQLException)
            return new JdbcResponse(((IgniteSQLException)e).statusCode(), e.getMessage());
        else
            return new JdbcResponse(IgniteQueryErrorCode.UNKNOWN, e.getMessage());
    }

    /**
     * Ordered batch worker.
     */
    private class OrderedBatchWorker extends GridWorker {
        /**
         * Constructor.
         */
        OrderedBatchWorker() {
            super(connCtx.kernalContext().igniteInstanceName(), "ordered-batch", JdbcRequestHandler.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            long nextBatchOrder = 0;

            while (true) {
                if (!cliCtx.isStream())
                    return;

                JdbcOrderedBatchExecuteRequest req;

                synchronized (orderedBatchesMux) {
                    req = orderedBatchesQueue.peek();

                    if (req == null || req.order() != nextBatchOrder) {
                        orderedBatchesMux.wait();

                        continue;
                    }
                    else
                        orderedBatchesQueue.poll();
                }

                executeBatchOrdered(req);

                nextBatchOrder++;
            }
        }
    }

    /**
     * Cancels query with specified request id;
     *
     * @param req Query cancellation request;
     * @return <code>QueryCancelledException</code> wrapped with <code>JdbcResponse</code>
     */
    private JdbcResponse cancelQuery(JdbcQueryCancelRequest req) {
        boolean clearCursors = false;

        GridQueryCancel cancelHook;

        synchronized (reqMux) {
            JdbcQueryDescriptor desc = reqRegister.get(req.requestIdToBeCancelled());

            // Query was already executed.
            if (desc == null)
                return null;

            // Query was registered, however execution didn't start yet.
            else if (!desc.isExecutionStarted()) {
                unregisterRequest(req.requestId());

                return exceptionToResult(new QueryCancelledException());
            }
            else {
                cancelHook = desc.cancelHook();

                desc.markCancelled();

                if (desc.usageCount() == 0) {
                    clearCursors = true;

                    unregisterRequest(req.requestIdToBeCancelled());
                }
            }
        }

        cancelHook.cancel();

        if (clearCursors)
            clearCursors(req.requestIdToBeCancelled());

        return null;
    }

    /**
     * Retrieve cache partitions distributions for given cache Ids.
     *
     * @param req <code>JdbcCachePartitionsRequest</code> that incapsulates set of cache Ids.
     * @return Partitions distributions.
     */
    private JdbcResponse getCachePartitions(JdbcCachePartitionsRequest req) {
        List<JdbcThinPartitionAwarenessMappingGroup> mappings = new ArrayList<>();

        AffinityTopologyVersion topVer = connCtx.kernalContext().cache().context().exchange().readyAffinityVersion();

        for (int cacheId : req.cacheIds()) {
            Map<UUID, Set<Integer>> partitionsMap = getPartitionsMap(
                connCtx.kernalContext().cache().cacheDescriptor(cacheId),
                topVer);

            mappings.add(new JdbcThinPartitionAwarenessMappingGroup(cacheId, partitionsMap));
        }

        return new JdbcResponse(new JdbcCachePartitionsResult(mappings), topVer);
    }

    /**
     * Get partition map for a cache.
     * @param cacheDesc Cache descriptor.
     * @return Partitions mapping for cache.
     */
    private Map<UUID, Set<Integer>> getPartitionsMap(DynamicCacheDescriptor cacheDesc, AffinityTopologyVersion affVer) {
        GridCacheContext cacheCtx = connCtx.kernalContext().cache().context().cacheContext(cacheDesc.cacheId());

        AffinityAssignment assignment = cacheCtx.affinity().assignment(affVer);

        Set<ClusterNode> nodes = assignment.primaryPartitionNodes();

        HashMap<UUID, Set<Integer>> res = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            UUID nodeId = node.id();
            Set<Integer> parts = assignment.primaryPartitions(nodeId);

            res.put(nodeId, parts);
        }

        return res;
    }

    /**
     * Checks whether query cancellation is supported whithin given version of protocal.
     *
     * @return True if supported, false otherwise.
     */
    @Override public boolean isCancellationSupported() {
        return (protocolVer.compareTo(VER_2_8_0) >= 0);
    }

    /**
     * Unregisters request if there are no cursors binded to it.
     *
     * @param reqId Reuest to unregist.
     */
    private void tryUnregisterRequest(long reqId) {
        assert isCancellationSupported();

        boolean unregisterReq = true;

        for (JdbcCursor cursor : jdbcCursors.values()) {
            if (cursor.requestId() == reqId) {
                unregisterReq = false;

                break;
            }
        }

        if (unregisterReq)
            unregisterRequest(reqId);
    }

    /**
     * Tries to close all cursors of request with given id and removes them from jdbcCursors map.
     *
     * @param reqId Request ID.
     */
    private void clearCursors(long reqId) {
        for (Iterator<Map.Entry<Long, JdbcCursor>> it = jdbcCursors.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, JdbcCursor> entry = it.next();

            JdbcCursor cursor = entry.getValue();

            if (cursor.requestId() == reqId) {
                try {
                    cursor.close();
                }
                catch (Exception e) {
                    U.error(log, "Failed to close cursor [reqId=" + reqId + ", cursor=" + cursor + ']', e);
                }

                it.remove();
            }
        }
    }

    /**
     * Checks whether query was cancelled - returns null if true, otherwise increments query descriptor usage count.
     *
     * @param cur Jdbc Cursor.
     * @return False, if query was already cancelled.
     */
    private boolean prepareQueryCancellationMeta(JdbcCursor cur) {
        if (isCancellationSupported()) {
            // Nothing to do - cursor was already removed.
            if (cur == null)
                return false;

            synchronized (reqMux) {
                JdbcQueryDescriptor desc = reqRegister.get(cur.requestId());

                // Query was already cancelled and unregisterd.
                if (desc == null)
                    return false;

                desc.incrementUsageCount();
            }
        }

        return true;
    }

    /**
     * Cleanups cursors or processors and unregistered request if necessary.
     *
     * @param unregisterReq Flag, that detecs whether it's necessary to unregister request.
     * @param reqId Request Id.
     */
    private void cleanupQueryCancellationMeta(boolean unregisterReq, long reqId) {
        if (isCancellationSupported()) {
            boolean clearCursors = false;

            synchronized (reqMux) {
                JdbcQueryDescriptor desc = reqRegister.get(reqId);

                if (desc != null) {
                    // Query was cancelled during execution.
                    if (desc.isCanceled()) {
                        clearCursors = true;

                        unregisterReq = true;
                    }
                    else
                        desc.decrementUsageCount();

                    if (unregisterReq)
                        unregisterRequest(reqId);
                }
            }

            if (clearCursors)
                clearCursors(reqId);
        }
    }

    /**
     * Create {@link JdbcResponse} wrapping given result and attaching affinity topology version to it if chaned.
     *
     * @param res Jdbc result.
     * @return esulting {@link JdbcResponse}.
     */
    private JdbcResponse resultToResonse(JdbcResult res) {
        return new JdbcResponse(res, connCtx.getAffinityTopologyVersionIfChanged());
    }

    /**
     * @param partResRequested Boolean flag that signals whether client requested partiton result.
     * @param partRes Direved partition result.
     * @return True if applicable to jdbc thin client side partition awareness:
     *   1. Partitoin result was requested;
     *   2. Partition result either null or
     *     a. Rendezvous affinity function without map filters was used;
     *     b. Partition result tree neither PartitoinAllNode nor PartitionNoneNode;
     */
    private static boolean isClientPartitionAwarenessApplicable(boolean partResRequested, PartitionResult partRes) {
        return partResRequested && (partRes == null || partRes.isClientPartitionAwarenessApplicable());
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProtocolVersion protocolVersion() {
        return protocolVer;
    }
}
