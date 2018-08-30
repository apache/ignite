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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.BulkLoadContextCursor;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCacheWriter;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParser;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.bulkload.BulkLoadStreamerWriter;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.StaticMvccQueryTracker;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.h2.dml.DmlBatchSender;
import org.apache.ignite.internal.processors.query.h2.dml.DmlDistributedPlanInfo;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdateMode;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.command.Prepared;
import org.h2.command.dml.Delete;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.Update;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccTracker;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.requestSnapshot;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;

/**
 *
 */
public class DmlStatementsProcessor {
    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private static final int DFLT_DML_RERUN_ATTEMPTS = 4;

    /** Indexing. */
    private IgniteH2Indexing idx;

    /** Logger. */
    private IgniteLogger log;

    /** Default size for update plan cache. */
    private static final int PLAN_CACHE_SIZE = 1024;

    /** Update plans cache. */
    private final ConcurrentMap<H2CachedStatementKey, UpdatePlan> planCache =
        new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param idx indexing.
     */
    public void start(GridKernalContext ctx, IgniteH2Indexing idx) {
        this.idx = idx;

        log = ctx.log(DmlStatementsProcessor.class);
    }

    /**
     * Handle cache stop.
     *
     * @param cacheName Cache name.
     */
    public void onCacheStop(String cacheName) {
        Iterator<Map.Entry<H2CachedStatementKey, UpdatePlan>> iter = planCache.entrySet().iterator();

        while (iter.hasNext()) {
            UpdatePlan plan = iter.next().getValue();

            if (F.eq(cacheName, plan.cacheContext().name()))
                iter.remove();
        }
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param schemaName Schema.
     * @param conn Connection.
     * @param prepared Prepared statement.
     * @param fieldsQry Original query.
     * @param loc Query locality flag.
     * @param filters Cache name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult updateSqlFields(String schemaName, Connection conn, Prepared prepared,
        SqlFieldsQuery fieldsQry, boolean loc, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        UpdatePlan plan = getPlanForStatement(schemaName, conn, prepared, fieldsQry, loc, null);

        GridCacheContext<?, ?> cctx = plan.cacheContext();

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            CacheOperationContext opCtx = setKeepBinaryContext(cctx);

            UpdateResult r;

            try {
                r = executeUpdateStatement(schemaName, plan, fieldsQry, loc, filters, cancel);
            }
            finally {
                cctx.operationContextPerCall(opCtx);
            }

            items += r.counter();
            errKeys = r.errorKeys();

            if (F.isEmpty(errKeys))
                break;
        }

        if (F.isEmpty(errKeys)) {
            if (items == 1L)
                return UpdateResult.ONE;
            else if (items == 0L)
                return UpdateResult.ZERO;
        }

        return new UpdateResult(items, errKeys);
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param schemaName Schema.
     * @param conn Connection.
     * @param prepared Prepared statement.
     * @param fieldsQry Original query.
     * @param loc Query locality flag.
     * @param filters Cache name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private Collection<UpdateResult> updateSqlFieldsBatched(String schemaName, Connection conn, Prepared prepared,
        SqlFieldsQueryEx fieldsQry, boolean loc, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        List<Object[]> argss = fieldsQry.batchedArguments();

        UpdatePlan plan = getPlanForStatement(schemaName, conn, prepared, fieldsQry, loc, null);

        GridCacheContext<?, ?> cctx = plan.cacheContext();

        // For MVCC case, let's enlist batch elements one by one.
        if (plan.hasRows() && plan.mode() == UpdateMode.INSERT && !cctx.mvccEnabled()) {
            CacheOperationContext opCtx = setKeepBinaryContext(cctx);

            try {
                List<List<List<?>>> cur = plan.createRows(argss);

                return processDmlSelectResultBatched(plan, cur, fieldsQry.getPageSize());
            }
            finally {
                cctx.operationContextPerCall(opCtx);
            }
        }
        else {
            // Fallback to previous mode.
            Collection<UpdateResult> ress = new ArrayList<>(argss.size());

            SQLException batchException = null;

            int[] cntPerRow = new int[argss.size()];

            int cntr = 0;

            for (Object[] args : argss) {
                SqlFieldsQueryEx qry0 = (SqlFieldsQueryEx)fieldsQry.copy();

                qry0.clearBatchedArgs();
                qry0.setArgs(args);

                UpdateResult res;

                try {
                    res = updateSqlFields(schemaName, conn, prepared, qry0, loc, filters, cancel);

                    cntPerRow[cntr++] = (int)res.counter();

                    ress.add(res);
                }
                catch (Exception e ) {
                    String sqlState;

                    int code;

                    if (e instanceof IgniteSQLException) {
                        sqlState = ((IgniteSQLException)e).sqlState();

                        code = ((IgniteSQLException)e).statusCode();
                    } else {
                        sqlState = SqlStateCode.INTERNAL_ERROR;

                        code = IgniteQueryErrorCode.UNKNOWN;
                    }

                    batchException = chainException(batchException, new SQLException(e.getMessage(), sqlState, code, e));

                    cntPerRow[cntr++] = Statement.EXECUTE_FAILED;
                }
            }

            if (batchException != null) {
                BatchUpdateException e = new BatchUpdateException(batchException.getMessage(),
                    batchException.getSQLState(), batchException.getErrorCode(), cntPerRow, batchException);

                throw new IgniteCheckedException(e);
            }

            return ress;
        }
    }

    /**
     * Makes current operation context as keepBinary.
     *
     * @param cctx Cache context.
     * @return Old operation context.
     */
    private CacheOperationContext setKeepBinaryContext(GridCacheContext<?, ?> cctx) {
        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null, false, true);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        return opCtx;
    }

    /**
     * @param schemaName Schema.
     * @param c Connection.
     * @param p Prepared statement.
     * @param fieldsQry Initial query
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    List<QueryCursorImpl<List<?>>> updateSqlFieldsDistributed(String schemaName, Connection c, Prepared p,
        SqlFieldsQuery fieldsQry, GridQueryCancel cancel) throws IgniteCheckedException {
        if (DmlUtils.isBatched(fieldsQry)) {
            Collection<UpdateResult> ress = updateSqlFieldsBatched(schemaName, c, p, (SqlFieldsQueryEx)fieldsQry,
                false, null, cancel);

            ArrayList<QueryCursorImpl<List<?>>> resCurs = new ArrayList<>(ress.size());

            for (UpdateResult res : ress) {
                checkUpdateResult(res);

                QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                    (Collections.singletonList(res.counter())), cancel, false);

                resCur.fieldsMeta(UPDATE_RESULT_META);

                resCurs.add(resCur);
            }

            return resCurs;
        }
        else {
            UpdateResult res = updateSqlFields(schemaName, c, p, fieldsQry, false, null, cancel);

            checkUpdateResult(res);

            QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                (Collections.singletonList(res.counter())), cancel, false);

            resCur.fieldsMeta(UPDATE_RESULT_META);

            return Collections.singletonList(resCur);
        }
    }

    /**
     * Execute DML statement on local cache.
     *
     * @param schemaName Schema.
     * @param conn Connection.
     * @param prepared H2 prepared command.
     * @param fieldsQry Fields query.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateSqlFieldsLocal(String schemaName, Connection conn, Prepared prepared,
        SqlFieldsQuery fieldsQry, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        UpdateResult res = updateSqlFields(schemaName, conn, prepared, fieldsQry, true,
            filters, cancel);

        return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META,
            new IgniteSingletonIterator(Collections.singletonList(res.counter())));
    }

    /**
     * Perform given statement against given data streamer. Only rows based INSERT is supported.
     *
     * @param schemaName Schema name.
     * @param streamer Streamer to feed data to.
     * @param stmt Statement.
     * @param args Statement arguments.
     * @return Number of rows in given INSERT statement.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    long streamUpdateQuery(String schemaName, IgniteDataStreamer streamer, PreparedStatement stmt, final Object[] args)
        throws IgniteCheckedException {
        idx.checkStatementStreamable(stmt);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        assert p != null;

        final UpdatePlan plan = getPlanForStatement(schemaName, null, p, null, true, null);

        assert plan.isLocalSubquery();

        final GridCacheContext cctx = plan.cacheContext();

        QueryCursorImpl<List<?>> cur;

        final ArrayList<List<?>> data = new ArrayList<>(plan.rowCount());

        QueryCursorImpl<List<?>> stepCur = new QueryCursorImpl<>(new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                try {
                    Iterator<List<?>> it;

                    if (!F.isEmpty(plan.selectQuery())) {
                        GridQueryFieldsResult res = idx.queryLocalSqlFields(idx.schema(cctx.name()),
                            plan.selectQuery(), F.asList(U.firstNotNull(args, X.EMPTY_OBJECT_ARRAY)),
                            null, false, false, 0, null);

                        it = res.iterator();
                    }
                    else
                        it = plan.createRows(U.firstNotNull(args, X.EMPTY_OBJECT_ARRAY)).iterator();

                    return new GridQueryCacheObjectsIterator(it, idx.objectContext(), cctx.keepBinary());
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, null);

        data.addAll(stepCur.getAll());

        cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                return data.iterator();
            }
        }, null);

        if (plan.rowCount() == 1) {
            IgniteBiTuple t = plan.processRow(cur.iterator().next());

            streamer.addData(t.getKey(), t.getValue());

            return 1;
        }

        Map<Object, Object> rows = new LinkedHashMap<>(plan.rowCount());

        for (List<?> row : cur) {
            final IgniteBiTuple t = plan.processRow(row);

            rows.put(t.getKey(), t.getValue());
        }

        streamer.addData(rows);

        return rows.size();
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param schemaName Schema name.
     * @param plan Cache context.
     * @param fieldsQry Fields query.
     * @param loc Local query flag.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel state holder.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private UpdateResult executeUpdateStatement(String schemaName, final UpdatePlan plan,
        SqlFieldsQuery fieldsQry, boolean loc, IndexingQueryFilter filters,
        GridQueryCancel cancel) throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        if (cctx != null && cctx.mvccEnabled()) {
            assert cctx.transactional();

            DmlDistributedPlanInfo distributedPlan = plan.distributedPlan();

            GridNearTxLocal tx = tx(cctx.kernalContext());

            boolean implicit = (tx == null);

            boolean commit = implicit && (!(fieldsQry instanceof SqlFieldsQueryEx) ||
                ((SqlFieldsQueryEx)fieldsQry).isAutoCommit());

            if (implicit)
                tx = txStart(cctx, fieldsQry.getTimeout());

            requestSnapshot(cctx, checkActive(tx));

            try (GridNearTxLocal toCommit = commit ? tx : null) {
                long timeout;

                if (implicit)
                    timeout = tx.remainingTime();
                else {
                    long tm1 = tx.remainingTime(), tm2 = fieldsQry.getTimeout();

                    timeout = tm1 > 0 && tm2 > 0 ? Math.min(tm1, tm2) : Math.max(tm1, tm2);
                }

                if (cctx.isReplicated() || distributedPlan == null || ((plan.mode() == UpdateMode.INSERT
                    || plan.mode() == UpdateMode.MERGE) && !plan.isLocalSubquery())) {

                    boolean sequential = true;

                    UpdateSourceIterator<?> it;

                    if (plan.fastResult()) {
                        IgniteBiTuple row = plan.getFastRow(fieldsQry.getArgs());

                        EnlistOperation op = UpdatePlan.enlistOperation(plan.mode());

                        it = new DmlUpdateSingleEntryIterator<>(op, op.isDeleteOrLock() ? row.getKey() : row);
                    }
                    else if (plan.hasRows())
                        it = new DmlUpdateResultsIterator(UpdatePlan.enlistOperation(plan.mode()), plan, plan.createRows(fieldsQry.getArgs()));
                    else {
                        // TODO IGNITE-8865 if there is no ORDER BY statement it's no use to retain entries order on locking (sequential = false).
                        SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fieldsQry.isCollocated())
                            .setArgs(fieldsQry.getArgs())
                            .setDistributedJoins(fieldsQry.isDistributedJoins())
                            .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                            .setLocal(fieldsQry.isLocal())
                            .setPageSize(fieldsQry.getPageSize())
                            .setTimeout((int)timeout, TimeUnit.MILLISECONDS);

                        FieldsQueryCursor<List<?>> cur = idx.querySqlFields(schemaName, newFieldsQry, null,
                            true, true, mvccTracker(cctx, tx), cancel).get(0);

                        it = plan.iteratorForTransaction(idx, cur);
                    }

                    IgniteInternalFuture<Long> fut = tx.updateAsync(cctx, it,
                        fieldsQry.getPageSize(), timeout, sequential);

                    UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY);

                    if (commit)
                        toCommit.commit();

                    return res;
                }

                int[] ids = U.toIntArray(distributedPlan.getCacheIds());

                int flags = 0;

                if (fieldsQry.isEnforceJoinOrder())
                    flags |= GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER;

                if (distributedPlan.isReplicatedOnly())
                    flags |= GridH2QueryRequest.FLAG_REPLICATED;

                int[] parts = fieldsQry.getPartitions();

                IgniteInternalFuture<Long> fut = tx.updateAsync(
                    cctx,
                    ids,
                    parts,
                    schemaName,
                    fieldsQry.getSql(),
                    fieldsQry.getArgs(),
                    flags,
                    fieldsQry.getPageSize(),
                    timeout);

                UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY);

                if (commit)
                    toCommit.commit();

                return res;
            }
            catch (IgniteCheckedException e) {
                checkSqlException(e);

                U.error(log, "Error during update [localNodeId=" + cctx.localNodeId() + "]", e);

                throw new IgniteSQLException("Failed to run update. " + e.getMessage(), e);
            }
            finally {
                if (commit)
                    cctx.tm().resetContext();
            }
        }

        UpdateResult fastUpdateRes = plan.processFast(fieldsQry.getArgs());

        if (fastUpdateRes != null)
            return fastUpdateRes;

        if (plan.distributedPlan() != null) {
            UpdateResult result = doDistributedUpdate(schemaName, fieldsQry, plan, cancel);

            // null is returned in case not all nodes support distributed DML.
            if (result != null)
                return result;
        }

        Iterable<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocalSubquery()) {
            assert !F.isEmpty(plan.selectQuery());

            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fieldsQry.isCollocated())
                .setArgs(fieldsQry.getArgs())
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>)idx.querySqlFields(schemaName, newFieldsQry, null, true, true,
                null, cancel).get(0);
        }
        else if (plan.hasRows())
            cur = plan.createRows(fieldsQry.getArgs());
        else {
            final GridQueryFieldsResult res = idx.queryLocalSqlFields(schemaName, plan.selectQuery(),
                F.asList(fieldsQry.getArgs()), filters, fieldsQry.isEnforceJoinOrder(), false, fieldsQry.getTimeout(),
                cancel);

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), idx.objectContext(), true);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);
        }

        int pageSize = loc ? 0 : fieldsQry.getPageSize();

        return processDmlSelectResult(plan, cur, pageSize);
    }

    /**
     * @param e Exception.
     */
    private void checkSqlException(IgniteCheckedException e) {
        IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

        if(sqlEx != null)
            throw sqlEx;
    }

    /**
     * Performs the planned update.
     * @param plan Update plan.
     * @param rows Rows to update.
     * @param pageSize Page size.
     * @return {@link List} of update results.
     * @throws IgniteCheckedException If failed.
     */
    private List<UpdateResult> processDmlSelectResultBatched(UpdatePlan plan, List<List<List<?>>> rows, int pageSize)
        throws IgniteCheckedException {
        switch (plan.mode()) {
            case MERGE:
                // TODO
                throw new IgniteCheckedException("Unsupported, fix");

            case INSERT:
                return doInsertBatched(plan, rows, pageSize);

            default:
                throw new IgniteSQLException("Unexpected batched DML operation [mode=" + plan.mode() + ']',
                    IgniteQueryErrorCode.UNEXPECTED_OPERATION);
        }
    }

    /**
     * @param plan Update plan.
     * @param cursor Cursor over select results.
     * @param pageSize Page size.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult processDmlSelectResult(UpdatePlan plan, Iterable<List<?>> cursor,
        int pageSize) throws IgniteCheckedException {
        switch (plan.mode()) {
            case MERGE:
                return new UpdateResult(doMerge(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

            case INSERT:
                return new UpdateResult(doInsert(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

            case UPDATE:
                return doUpdate(plan, cursor, pageSize);

            case DELETE:
                return doDelete(plan.cacheContext(), cursor, pageSize);

            default:
                throw new IgniteSQLException("Unexpected DML operation [mode=" + plan.mode() + ']',
                    IgniteQueryErrorCode.UNEXPECTED_OPERATION);
        }
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param schema Schema.
     * @param conn Connection.
     * @param p Prepared statement.
     * @param fieldsQry Original fields query.
     * @param loc Local query flag.
     * @return Update plan.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    UpdatePlan getPlanForStatement(String schema, Connection conn, Prepared p, SqlFieldsQuery fieldsQry,
        boolean loc, @Nullable Integer errKeysPos) throws IgniteCheckedException {
        isDmlOnSchemaSupported(schema);

        H2CachedStatementKey planKey = H2CachedStatementKey.forDmlStatement(schema, p.getSQL(), fieldsQry, loc);

        UpdatePlan res = (errKeysPos == null ? planCache.get(planKey) : null);

        if (res != null)
            return res;

        res = UpdatePlanBuilder.planForStatement(p, loc, idx, conn, fieldsQry, errKeysPos);

        // Don't cache re-runs
        if (errKeysPos == null)
            return U.firstNotNull(planCache.putIfAbsent(planKey, res), res);
        else
            return res;
    }

    /**
     * @param schemaName Schema name.
     * @param fieldsQry Initial query.
     * @param plan Update plan.
     * @param cancel Cancel state.
     * @return Update result.
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult doDistributedUpdate(String schemaName, SqlFieldsQuery fieldsQry, UpdatePlan plan,
        GridQueryCancel cancel) throws IgniteCheckedException {
        DmlDistributedPlanInfo distributedPlan = plan.distributedPlan();

        assert distributedPlan != null;

        if (cancel == null)
            cancel = new GridQueryCancel();

        return idx.runDistributedUpdate(schemaName, fieldsQry, distributedPlan.getCacheIds(),
            distributedPlan.isReplicatedOnly(), cancel);
    }

    /**
     * Perform DELETE operation on top of results of SELECT.
     * @param cctx Cache context.
     * @param cursor SELECT results.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Results of DELETE (number of items affected AND keys that failed to be updated).
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ThrowableResultOfMethodCallIgnored"})
    private UpdateResult doDelete(GridCacheContext cctx, Iterable<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

        for (List<?> row : cursor) {
            if (row.size() != 2) {
                U.warn(log, "Invalid row size on DELETE - expected 2, got " + row.size());

                continue;
            }

            sender.add(row.get(0), new ModifyingEntryProcessor(row.get(1), RMV),  0);
        }

        sender.flush();

        SQLException resEx = sender.error();

        if (resEx != null) {
            if (!F.isEmpty(sender.failedKeys())) {
                // Don't go for a re-run if processing of some keys yielded exceptions and report keys that
                // had been modified concurrently right away.
                String msg = "Failed to DELETE some keys because they had been modified concurrently " +
                    "[keys=" + sender.failedKeys() + ']';

                SQLException conEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

                conEx.setNextException(resEx);

                resEx = conEx;
            }

            throw new IgniteSQLException(resEx);
        }

        return new UpdateResult(sender.updateCount(), sender.failedKeys().toArray());
    }

    /**
     * Perform UPDATE operation on top of results of SELECT.
     * @param cursor SELECT results.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Pair [cursor corresponding to results of UPDATE (contains number of items affected); keys whose values
     *     had been modified concurrently (arguments for a re-run)].
     */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    private UpdateResult doUpdate(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

        for (List<?> row : cursor) {
            T3<Object, Object, Object> row0 = plan.processRowForUpdate(row);

            Object key = row0.get1();
            Object oldVal = row0.get2();
            Object newVal = row0.get3();

            sender.add(key, new ModifyingEntryProcessor(oldVal, new EntryValueUpdater(newVal)), 0);
        }

        sender.flush();

        SQLException resEx = sender.error();

        if (resEx != null) {
            if (!F.isEmpty(sender.failedKeys())) {
                // Don't go for a re-run if processing of some keys yielded exceptions and report keys that
                // had been modified concurrently right away.
                String msg = "Failed to UPDATE some keys because they had been modified concurrently " +
                    "[keys=" + sender.failedKeys() + ']';

                SQLException dupEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

                dupEx.setNextException(resEx);

                resEx = dupEx;
            }

            throw new IgniteSQLException(resEx);
        }

        return new UpdateResult(sender.updateCount(), sender.failedKeys().toArray());
    }

    /**
     * Execute MERGE statement plan.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size to stream data from {@code cursor}, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private long doMerge(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        // If we have just one item to put, just do so
        if (plan.rowCount() == 1) {
            IgniteBiTuple t = plan.processRow(cursor.iterator().next());

            cctx.cache().put(t.getKey(), t.getValue());

            return 1;
        }
        else {
            int resCnt = 0;

            Map<Object, Object> rows = new LinkedHashMap<>();

            for (Iterator<List<?>> it = cursor.iterator(); it.hasNext();) {
                List<?> row = it.next();

                IgniteBiTuple t = plan.processRow(row);

                rows.put(t.getKey(), t.getValue());

                if ((pageSize > 0 && rows.size() == pageSize) || !it.hasNext()) {
                    cctx.cache().putAll(rows);

                    resCnt += rows.size();

                    if (it.hasNext())
                        rows.clear();
                }
            }

            return resCnt;
        }
    }

    /**
     * Execute INSERT statement plan.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private long doInsert(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        // If we have just one item to put, just do so
        if (plan.rowCount() == 1) {
            IgniteBiTuple t = plan.processRow(cursor.iterator().next());

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw new IgniteSQLException("Duplicate key during INSERT [key=" + t.getKey() + ']',
                    DUPLICATE_KEY);
        }
        else {
            // Keys that failed to INSERT due to duplication.
            DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

            for (List<?> row : cursor) {
                final IgniteBiTuple keyValPair = plan.processRow(row);

                sender.add(keyValPair.getKey(), new InsertEntryProcessor(keyValPair.getValue()),  0);
            }

            sender.flush();

            SQLException resEx = sender.error();

            if (!F.isEmpty(sender.failedKeys())) {
                String msg = "Failed to INSERT some keys because they are already in cache " +
                    "[keys=" + sender.failedKeys() + ']';

                SQLException dupEx = new SQLException(msg, SqlStateCode.CONSTRAINT_VIOLATION);

                if (resEx == null)
                    resEx = dupEx;
                else
                    resEx.setNextException(dupEx);
            }

            if (resEx != null)
                throw new IgniteSQLException(resEx);

            return sender.updateCount();
        }
    }

    /**
     * Execute INSERT statement plan.
     *
     * @param plan Plan to execute.
     * @param cursor Cursor to take inserted data from. I.e. list of batch arguments for each query.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    private List<UpdateResult> doInsertBatched(UpdatePlan plan, List<List<List<?>>> cursor, int pageSize)
        throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        DmlBatchSender snd = new DmlBatchSender(cctx, pageSize, cursor.size());

        int rowNum = 0;

        SQLException resEx = null;

        for (List<List<?>> qryRow : cursor) {
            for (List<?> row : qryRow) {
                try {
                    final IgniteBiTuple keyValPair = plan.processRow(row);

                    snd.add(keyValPair.getKey(), new InsertEntryProcessor(keyValPair.getValue()), rowNum);
                }
                catch (Exception e) {
                    String sqlState;

                    int code;

                    if (e instanceof IgniteSQLException) {
                        sqlState = ((IgniteSQLException)e).sqlState();

                        code = ((IgniteSQLException)e).statusCode();
                    } else {
                        sqlState = SqlStateCode.INTERNAL_ERROR;

                        code = IgniteQueryErrorCode.UNKNOWN;
                    }

                    resEx = chainException(resEx, new SQLException(e.getMessage(), sqlState, code, e));

                    snd.setFailed(rowNum);
                }
            }

            rowNum++;
        }

        try {
            snd.flush();
        }
        catch (Exception e) {
            resEx = chainException(resEx, new SQLException(e.getMessage(), SqlStateCode.INTERNAL_ERROR,
                IgniteQueryErrorCode.UNKNOWN, e));
        }

        resEx = chainException(resEx, snd.error());

        if (!F.isEmpty(snd.failedKeys())) {
            SQLException e = new SQLException("Failed to INSERT some keys because they are already in cache [keys=" +
                snd.failedKeys() + ']', SqlStateCode.CONSTRAINT_VIOLATION, DUPLICATE_KEY);

            resEx = chainException(resEx, e);
        }

        if (resEx != null) {
            BatchUpdateException e = new BatchUpdateException(resEx.getMessage(), resEx.getSQLState(),
                resEx.getErrorCode(), snd.perRowCounterAsArray(), resEx);

            throw new IgniteCheckedException(e);
        }

        int[] cntPerRow = snd.perRowCounterAsArray();

        List<UpdateResult> res = new ArrayList<>(cntPerRow.length);

        for (int i = 0; i < cntPerRow.length; i++ ) {
            int cnt = cntPerRow[i];

            res.add(new UpdateResult(cnt , X.EMPTY_OBJECT_ARRAY));
        }

        return res;
    }

    /**
     * Adds exception to the chain.
     *
     * @param main Exception to add another exception to.
     * @param add Exception which should be added to chain.
     * @return Chained exception.
     */
    private SQLException chainException(SQLException main, SQLException add) {
        if (main == null) {
            if (add != null) {
                main = add;

                return main;
            }
            else
                return null;
        }
        else {
            main.setNextException(add);

            return main;
        }
    }

    /**
     *
     * @param schemaName Schema name.
     * @param stmt Prepared statement.
     * @param fldsQry Query.
     * @param filter Filter.
     * @param cancel Cancel state.
     * @param local Locality flag.
     * @return Update result.
     * @throws IgniteCheckedException if failed.
     */
    UpdateResult mapDistributedUpdate(String schemaName, PreparedStatement stmt, SqlFieldsQuery fldsQry,
        IndexingQueryFilter filter, GridQueryCancel cancel, boolean local) throws IgniteCheckedException {
        Connection c;

        try {
            c = stmt.getConnection();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }

        return updateSqlFields(schemaName, c, GridSqlQueryParser.prepared(stmt), fldsQry, local, filter, cancel);
    }

    /**
     * @param schema Schema name.
     * @param conn Connection.
     * @param stmt Prepared statement.
     * @param qry Sql fields query
     * @param filter Backup filter.
     * @param cancel Query cancel object.
     * @param local {@code true} if should be executed locally.
     * @param topVer Topology version.
     * @param mvccSnapshot MVCC snapshot.
     * @return Iterator upon updated values.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public UpdateSourceIterator<?> prepareDistributedUpdate(String schema, Connection conn,
        PreparedStatement stmt, SqlFieldsQuery qry,
        IndexingQueryFilter filter, GridQueryCancel cancel, boolean local,
        AffinityTopologyVersion topVer, MvccSnapshot mvccSnapshot) throws IgniteCheckedException {

        Prepared prepared = GridSqlQueryParser.prepared(stmt);

        UpdatePlan plan = getPlanForStatement(schema, conn, prepared, qry, local, null);

        GridCacheContext cctx = plan.cacheContext();

        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                newOpCtx = new CacheOperationContext().keepBinary();
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!local && !plan.isLocalSubquery()) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), qry.isCollocated())
                .setArgs(qry.getArgs())
                .setDistributedJoins(qry.isDistributedJoins())
                .setEnforceJoinOrder(qry.isEnforceJoinOrder())
                .setLocal(qry.isLocal())
                .setPageSize(qry.getPageSize())
                .setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>)idx.querySqlFields(schema, newFieldsQry, null, true, true,
                new StaticMvccQueryTracker(cctx, mvccSnapshot), cancel).get(0);
        }
        else {
            final GridQueryFieldsResult res = idx.queryLocalSqlFields(schema, plan.selectQuery(),
                F.asList(qry.getArgs()), filter, qry.isEnforceJoinOrder(), false, qry.getTimeout(), cancel,
                new StaticMvccQueryTracker(cctx, mvccSnapshot));

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return res.iterator();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);
        }

        return plan.iteratorForTransaction(idx, cur);
    }

    /**
     * Runs a DML statement for which we have internal command executor.
     *
     * @param sql The SQL command text to execute.
     * @param cmd The command to execute.
     * @return The cursor returned by the statement.
     * @throws IgniteSQLException If failed.
     */
    public FieldsQueryCursor<List<?>> runNativeDmlStatement(String sql, SqlCommand cmd) {
        try {
            if (cmd instanceof SqlBulkLoadCommand)
                return processBulkLoadCommand((SqlBulkLoadCommand)cmd);
            else
                throw new IgniteSQLException("Unsupported DML operation: " + sql,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Unexpected DML operation failure: " + e.getMessage(), e);
        }
    }

    /**
     * Process bulk load COPY command.
     *
     * @param cmd The command.
     * @return The context (which is the result of the first request/response).
     * @throws IgniteCheckedException If something failed.
     */
    public FieldsQueryCursor<List<?>> processBulkLoadCommand(SqlBulkLoadCommand cmd) throws IgniteCheckedException {
        if (cmd.packetSize() == null)
            cmd.packetSize(BulkLoadAckClientParameters.DFLT_PACKET_SIZE);

        GridH2Table tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());

        if (tbl == null) {
            idx.kernalContext().cache().createMissingQueryCaches();

            tbl = idx.dataTable(cmd.schemaName(), cmd.tableName());
        }

        if (tbl == null) {
            throw new IgniteSQLException("Table does not exist: " + cmd.tableName(),
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        UpdatePlan plan = UpdatePlanBuilder.planForBulkLoad(cmd, tbl);

        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter = new BulkLoadDataConverter(plan);

        GridCacheContext cache = tbl.cache();

        IgniteDataStreamer<Object, Object> streamer = cache.grid().dataStreamer(cache.name());

        BulkLoadCacheWriter outputWriter = new BulkLoadStreamerWriter(streamer);

        BulkLoadParser inputParser = BulkLoadParser.createParser(cmd.inputFormat());

        BulkLoadProcessor processor = new BulkLoadProcessor(inputParser, dataConverter, outputWriter);

        BulkLoadAckClientParameters params = new BulkLoadAckClientParameters(cmd.localFileName(), cmd.packetSize());

        return new BulkLoadContextCursor(processor, params);
    }

    /** */
    private static final class InsertEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to set. */
        private final Object val;

        /** */
        private InsertEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            if (entry.exists())
                return false;

            entry.setValue(val);
            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /**
     * Entry processor invoked by UPDATE and DELETE operations.
     */
    private static final class ModifyingEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to expect. */
        private final Object val;

        /** Action to perform on entry. */
        private final IgniteInClosure<MutableEntry<Object, Object>> entryModifier;

        /** */
        private ModifyingEntryProcessor(Object val, IgniteInClosure<MutableEntry<Object, Object>> entryModifier) {
            assert val != null;

            this.val = val;
            this.entryModifier = entryModifier;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            if (!entry.exists())
                return null; // Someone got ahead of us and removed this entry, let's skip it.

            Object entryVal = entry.getValue();

            if (entryVal == null)
                return null;

            // Something happened to the cache while we were performing map-reduce.
            if (!F.eq(entryVal, val))
                return false;

            entryModifier.apply(entry);

            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /** */
    private static IgniteInClosure<MutableEntry<Object, Object>> RMV = new IgniteInClosure<MutableEntry<Object, Object>>() {
        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.remove();
        }
    };

    /**
     *
     */
    private static final class EntryValueUpdater implements IgniteInClosure<MutableEntry<Object, Object>> {
        /** Value to set. */
        private final Object val;

        /** */
        private EntryValueUpdater(Object val) {
            assert val != null;

            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.setValue(val);
        }
    }

    /**
     * Check whether statement is DML statement.
     *
     * @param stmt Statement.
     * @return {@code True} if this is DML.
     */
    static boolean isDmlStatement(Prepared stmt) {
        return stmt instanceof Merge || stmt instanceof Insert || stmt instanceof Update || stmt instanceof Delete;
    }

    /**
     * Check if schema supports DDL statement.
     *
     * @param schemaName Schema name.
     */
    private static void isDmlOnSchemaSupported(String schemaName) {
        if (F.eq(QueryUtils.SCHEMA_SYS, schemaName))
            throw new IgniteSQLException("DML statements are not supported on " + schemaName + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Check update result for erroneous keys and throws concurrent update exception if necessary.
     *
     * @param r Update result.
     */
    static void checkUpdateResult(UpdateResult r) {
        if (!F.isEmpty(r.errorKeys())) {
            String msg = "Failed to update some keys because they had been modified concurrently " +
                "[keys=" + Arrays.toString(r.errorKeys()) + ']';

            SQLException conEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

            throw new IgniteSQLException(conEx);
        }
    }

    /**
     * Converts a row of values to actual key+value using {@link UpdatePlan#processRow(List)}.
     */
    private static class BulkLoadDataConverter extends IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> {
        /** Update plan to convert incoming rows. */
        private final UpdatePlan plan;

        /**
         * Creates the converter with the given update plan.
         *
         * @param plan The update plan to use.
         */
        private BulkLoadDataConverter(UpdatePlan plan) {
            this.plan = plan;
        }

        /**
         * Converts the record to a key+value.
         *
         * @param record The record to convert.
         * @return The key+value.
         * @throws IgniteCheckedException If conversion failed for some reason.
         */
        @Override public IgniteBiTuple<?, ?> applyx(List<?> record) throws IgniteCheckedException {
            return plan.processRow(record);
        }
    }

    /** */
    private static class DmlUpdateResultsIterator
        implements UpdateSourceIterator<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private EnlistOperation op;

        /** */
        private UpdatePlan plan;

        /** */
        private Iterator<List<?>> it;

        /** */
        DmlUpdateResultsIterator(EnlistOperation op, UpdatePlan plan, Iterable<List<?>> rows) {
            this.op = op;
            this.plan = plan;
            this.it = rows.iterator();
        }

        /** {@inheritDoc} */
        @Override public EnlistOperation operation() {
            return op;
        }

        /** {@inheritDoc} */
        public boolean hasNextX() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        public Object nextX() throws IgniteCheckedException {
            return plan.processRowForTx(it.next());
        }
    }

    /** */
    private static class DmlUpdateSingleEntryIterator<T> implements UpdateSourceIterator<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private EnlistOperation op;

        /** */
        private boolean first = true;

        /** */
        private T entry;

        /** */
        DmlUpdateSingleEntryIterator(EnlistOperation op, T entry) {
            this.op = op;
            this.entry = entry;
        }

        /** {@inheritDoc} */
        @Override public EnlistOperation operation() {
            return op;
        }

        /** {@inheritDoc} */
        public boolean hasNextX() {
            return first;
        }

        /** {@inheritDoc} */
        public T nextX() {
            T res = first ? entry : null;

            first = false;

            return res;
        }
    }
}
