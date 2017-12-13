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

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.dml.FastUpdateArguments;
import org.apache.ignite.internal.processors.query.h2.dml.UpdateMode;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
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
import org.h2.table.Column;
import org.h2.util.DateTimeUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 *
 */
public class DmlStatementsProcessor {
    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private final static int DFLT_DML_RERUN_ATTEMPTS = 4;

    /** Indexing. */
    private IgniteH2Indexing idx;

    /** Logger. */
    private IgniteLogger log;

    /** Default size for update plan cache. */
    private static final int PLAN_CACHE_SIZE = 1024;

    /** Update plans cache. */
    private final ConcurrentMap<H2DmlPlanKey, UpdatePlan> planCache =
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
        Iterator<Map.Entry<H2DmlPlanKey, UpdatePlan>> iter = planCache.entrySet().iterator();

        while (iter.hasNext()) {
            UpdatePlan plan = iter.next().getValue();

            if (F.eq(cacheName, plan.tbl.cacheName()))
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

        GridCacheContext<?, ?> cctx = plan.tbl.rowDescriptor().context();

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            CacheOperationContext opCtx = cctx.operationContextPerCall();

            // Force keepBinary for operation context to avoid binary deserialization inside entry processor
            if (cctx.binaryMarshaller()) {
                CacheOperationContext newOpCtx = null;

                if (opCtx == null)
                    // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                    newOpCtx = new CacheOperationContext(false, null, true, null, false, null, false);
                else if (!opCtx.isKeepBinary())
                    newOpCtx = opCtx.keepBinary();

                if (newOpCtx != null)
                    cctx.operationContextPerCall(newOpCtx);
            }

            UpdateResult r;

            try {
                r = executeUpdateStatement(schemaName, cctx, conn, prepared, fieldsQry, loc, filters, cancel, errKeys);
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
     * @param schemaName Schema.
     * @param c Connection.
     * @param p Prepared statement.
     * @param fieldsQry Initial query
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    QueryCursorImpl<List<?>> updateSqlFieldsDistributed(String schemaName, Connection c, Prepared p,
        SqlFieldsQuery fieldsQry, GridQueryCancel cancel) throws IgniteCheckedException {
        UpdateResult res = updateSqlFields(schemaName, c, p, fieldsQry, false, null, cancel);

        checkUpdateResult(res);

        QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
            (Collections.singletonList(res.counter())), cancel, false);

        resCur.fieldsMeta(UPDATE_RESULT_META);

        return resCur;
    }

    /**
     * Execute DML statement on local cache.
     *
     * @param schemaName Schema.
     * @param conn Connection.
     * @param stmt Prepared statement.
     * @param fieldsQry Fields query.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateSqlFieldsLocal(String schemaName, Connection conn, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        UpdateResult res = updateSqlFields(schemaName, conn, GridSqlQueryParser.prepared(stmt), fieldsQry, true,
            filters, cancel);

        return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META,
            new IgniteSingletonIterator(Collections.singletonList(res.counter())));
    }

    /**
     * Perform given statement against given data streamer. Only rows based INSERT is supported.
     *
     * @param streamer Streamer to feed data to.
     * @param stmt Statement.
     * @param args Statement arguments.
     * @return Number of rows in given INSERT statement.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    long streamUpdateQuery(IgniteDataStreamer streamer, PreparedStatement stmt, Object[] args)
        throws IgniteCheckedException {
        args = U.firstNotNull(args, X.EMPTY_OBJECT_ARRAY);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        assert p != null;

        UpdatePlan plan = UpdatePlanBuilder.planForStatement(p, true, idx, null, null, null);

        if (!F.eq(streamer.cacheName(), plan.tbl.rowDescriptor().context().name()))
            throw new IgniteSQLException("Cross cache streaming is not supported, please specify cache explicitly" +
                " in connection options", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        if (plan.mode == UpdateMode.INSERT && plan.rowsNum > 0) {
            assert plan.isLocSubqry;

            final GridCacheContext cctx = plan.tbl.rowDescriptor().context();

            QueryCursorImpl<List<?>> cur;

            final ArrayList<List<?>> data = new ArrayList<>(plan.rowsNum);

            final GridQueryFieldsResult res = idx.queryLocalSqlFields(idx.schema(cctx.name()), plan.selectQry,
                F.asList(args), null, false, 0, null);

            QueryCursorImpl<List<?>> stepCur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), idx.objectContext(),
                            cctx.keepBinary());
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

            if (plan.rowsNum == 1) {
                IgniteBiTuple t = rowToKeyValue(cctx, cur.iterator().next(), plan);

                streamer.addData(t.getKey(), t.getValue());

                return 1;
            }

            Map<Object, Object> rows = new LinkedHashMap<>(plan.rowsNum);

            for (List<?> row : cur) {
                final IgniteBiTuple t = rowToKeyValue(cctx, row, plan);

                rows.put(t.getKey(), t.getValue());
            }

            streamer.addData(rows);

            return rows.size();
        }
        else
            throw new IgniteSQLException("Only tuple based INSERT statements are supported in streaming mode",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param schemaName Schema name.
     * @param cctx Cache context.
     * @param c Connection.
     * @param prepared Prepared statement for DML query.
     * @param fieldsQry Fields query.
     * @param loc Local query flag.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel state holder.
     * @param failedKeys Keys to restrict UPDATE and DELETE operations with. Null or empty array means no restriction.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private UpdateResult executeUpdateStatement(String schemaName, final GridCacheContext cctx, Connection c,
        Prepared prepared, SqlFieldsQuery fieldsQry, boolean loc, IndexingQueryFilter filters,
        GridQueryCancel cancel, Object[] failedKeys) throws IgniteCheckedException {
        int mainCacheId = cctx.cacheId();

        Integer errKeysPos = null;

        UpdatePlan plan = getPlanForStatement(schemaName, c, prepared, fieldsQry, loc, errKeysPos);

        GridCacheContext cctx0 = plan.tbl.cache();

        if (cctx0.mvccEnabled() && cctx0.transactional()) {
            GridNearTxLocal tx = cctx0.tm().userTx();

            if (tx != null) {
                int flags = fieldsQry.isEnforceJoinOrder() ? GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER : 0;

                int[] ids;

                if (plan.distributed != null) {
                    List<Integer> cacheIds = plan.distributed.getCacheIds();

                    ids = new int[cacheIds.size()];

                    for (int i = 0; i < ids.length; i++)
                        ids[i] = cacheIds.get(i);

                    if (plan.distributed.isReplicatedOnly())
                        flags |= GridH2QueryRequest.FLAG_REPLICATED;
                }
                else
                    ids = collectCacheIds(cctx0.cacheId(), prepared);

                long tm1 = tx.remainingTime(), tm2 = fieldsQry.getTimeout();

                long timeout = tm1 > 0 && tm2 > 0 ? Math.min(tm1, tm2) : tm1 > 0 ? tm1 : tm2;

                IgniteInternalFuture<Long> fut = tx.updateAsync(
                    cctx0,
                    ids,
                    fieldsQry.getPartitions(),
                    schemaName,
                    fieldsQry.getSql(),
                    fieldsQry.getArgs(),
                    flags,
                    fieldsQry.getPageSize(),
                    timeout);

                try {
                    return new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error during update [localNodeId=" + cctx0.localNodeId() + "]", e);

                    throw new CacheException("Failed to run update. " + e.getMessage(), e);
                }
            }
        }

        if (plan.fastUpdateArgs != null) {
            assert F.isEmpty(failedKeys) && errKeysPos == null;

            return doFastUpdate(plan, fieldsQry.getArgs());
        }

        if (plan.distributed != null) {
            UpdateResult result = doDistributedUpdate(schemaName, fieldsQry, plan, cancel);

            // null is returned in case not all nodes support distributed DML.
            if (result != null)
                return result;
        }

        assert !F.isEmpty(plan.selectQry);

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocSubqry) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQry, fieldsQry.isCollocated())
                .setArgs(fieldsQry.getArgs())
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>)idx.queryDistributedSqlFields(schemaName, newFieldsQry, true,
                cancel, mainCacheId, true).get(0);
        }
        else {
            final GridQueryFieldsResult res = idx.queryLocalSqlFields(schemaName, plan.selectQry,
                F.asList(fieldsQry.getArgs()), filters, fieldsQry.isEnforceJoinOrder(), fieldsQry.getTimeout(), cancel);

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

        return processDmlSelectResult(cctx, plan, cur, pageSize);
    }

    /** */
    private int[] collectCacheIds(int mainCacheId, Prepared p) {
        GridSqlQueryParser parser = new GridSqlQueryParser(false);

        parser.parse(p);

        Collection<Integer> ids = new HashSet<>();

        GridCacheContext cctx = null;
        boolean mvccEnabled = false;

        // check all involved caches
        for (Object o : parser.objectsMap().values()) {
            if (o instanceof GridSqlInsert)
                o = ((GridSqlInsert)o).into();
            else if (o instanceof GridSqlMerge)
                o = ((GridSqlMerge)o).into();
            else if (o instanceof GridSqlDelete)
                o = ((GridSqlDelete)o).from();

            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);

            if (o instanceof GridSqlTable) {
                if (cctx == null)
                    mvccEnabled = (cctx = (((GridSqlTable)o).dataTable()).cache()).mvccEnabled();
                else if ((cctx = (((GridSqlTable)o).dataTable()).cache()).mvccEnabled() != mvccEnabled)
                    throw new IllegalStateException("Using caches with different mvcc settings in same query is forbidden.");

                ids.add(cctx.cacheId());
            }
        }

        int cntr = ids.size(); int[] res = new int[cntr];

        for (Integer id : ids)
            res[id == mainCacheId ? 0 : --cntr] = id;

        assert cntr == 1;

        return res;
    }

    /**
     * @param cctx Cache context.
     * @param plan Update plan.
     * @param cursor Cursor over select results.
     * @param pageSize Page size.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult processDmlSelectResult(GridCacheContext cctx, UpdatePlan plan, Iterable<List<?>> cursor,
        int pageSize) throws IgniteCheckedException {
        switch (plan.mode) {
            case MERGE:
                return new UpdateResult(doMerge(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

            case INSERT:
                return new UpdateResult(doInsert(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

            case UPDATE:
                return doUpdate(plan, cursor, pageSize);

            case DELETE:
                return doDelete(cctx, cursor, pageSize);

            default:
                throw new IgniteSQLException("Unexpected DML operation [mode=" + plan.mode + ']',
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
    private UpdatePlan getPlanForStatement(String schema, Connection conn, Prepared p, SqlFieldsQuery fieldsQry,
        boolean loc, @Nullable Integer errKeysPos) throws IgniteCheckedException {
        H2DmlPlanKey planKey = new H2DmlPlanKey(schema, p.getSQL(), loc, fieldsQry);

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
     * Perform single cache operation based on given args.
     * @param plan Update plan.
     * @param args Query parameters.
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static UpdateResult doFastUpdate(UpdatePlan plan, Object[] args) throws IgniteCheckedException {
        GridCacheContext cctx = plan.tbl.rowDescriptor().context();

        FastUpdateArguments singleUpdate = plan.fastUpdateArgs;

        assert singleUpdate != null;

        boolean valBounded = (singleUpdate.val != FastUpdateArguments.NULL_ARGUMENT);

        if (singleUpdate.newVal != FastUpdateArguments.NULL_ARGUMENT) { // Single item UPDATE
            Object key = singleUpdate.key.apply(args);
            Object newVal = singleUpdate.newVal.apply(args);

            if (valBounded) {
                Object val = singleUpdate.val.apply(args);

                return (cctx.cache().replace(key, val, newVal) ? UpdateResult.ONE : UpdateResult.ZERO);
            }
            else
                return (cctx.cache().replace(key, newVal) ? UpdateResult.ONE : UpdateResult.ZERO);
        }
        else { // Single item DELETE
            Object key = singleUpdate.key.apply(args);
            Object val = singleUpdate.val.apply(args);

            if (singleUpdate.val == FastUpdateArguments.NULL_ARGUMENT) // No _val bound in source query
                return cctx.cache().remove(key) ? UpdateResult.ONE : UpdateResult.ZERO;
            else
                return cctx.cache().remove(key, val) ? UpdateResult.ONE : UpdateResult.ZERO;
        }
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
        assert plan.distributed != null;

        if (cancel == null)
            cancel = new GridQueryCancel();

        return idx.runDistributedUpdate(schemaName, fieldsQry, plan.distributed.getCacheIds(),
            plan.distributed.isReplicatedOnly(), cancel);
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
        BatchSender sender = new BatchSender(cctx, pageSize);

        for (List<?> row : cursor) {
            if (row.size() != 2) {
                U.warn(log, "Invalid row size on DELETE - expected 2, got " + row.size());

                continue;
            }

            sender.add(row.get(0), new ModifyingEntryProcessor(row.get(1), RMV));
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
        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        boolean bin = cctx.binaryMarshaller();

        String[] updatedColNames = plan.colNames;

        int valColIdx = plan.valColIdx;

        boolean hasNewVal = (valColIdx != -1);

        // Statement updates distinct properties if it does not have _val in updated columns list
        // or if its list of updated columns includes only _val, i.e. is single element.
        boolean hasProps = !hasNewVal || updatedColNames.length > 1;

        BatchSender sender = new BatchSender(cctx, pageSize);

        for (List<?> row : cursor) {
            Object key = row.get(0);

            Object newVal;

            Map<String, Object> newColVals = new HashMap<>();

            for (int i = 0; i < plan.colNames.length; i++) {
                if (hasNewVal && i == valColIdx - 2)
                    continue;

                GridQueryProperty prop = plan.tbl.rowDescriptor().type().property(plan.colNames[i]);

                assert prop != null : "Unknown property: " + plan.colNames[i];

                newColVals.put(plan.colNames[i], convert(row.get(i + 2), desc, prop.type(), plan.colTypes[i]));
            }

            newVal = plan.valSupplier.apply(row);

            if (newVal == null)
                throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

            // Skip key and value - that's why we start off with 3rd column
            for (int i = 0; i < plan.tbl.getColumns().length - DEFAULT_COLUMNS_COUNT; i++) {
                Column c = plan.tbl.getColumn(i + DEFAULT_COLUMNS_COUNT);

                if (desc.isKeyValueOrVersionColumn(c.getColumnId()))
                    continue;

                GridQueryProperty prop = desc.type().property(c.getName());

                if (prop.key())
                    continue; // Don't get values of key's columns - we won't use them anyway

                boolean hasNewColVal = newColVals.containsKey(c.getName());

                if (!hasNewColVal)
                    continue;

                Object colVal = newColVals.get(c.getName());

                // UPDATE currently does not allow to modify key or its fields, so we must be safe to pass null as key.
                desc.setColumnValue(null, newVal, colVal, i);
            }

            if (bin && hasProps) {
                assert newVal instanceof BinaryObjectBuilder;

                newVal = ((BinaryObjectBuilder) newVal).build();
            }

            desc.type().validateKeyAndValue(key, newVal);

            Object srcVal = row.get(1);

            if (bin && !(srcVal instanceof BinaryObject))
                srcVal = cctx.grid().binary().toBinary(srcVal);

            sender.add(key, new ModifyingEntryProcessor(srcVal, new EntryValueUpdater(newVal)));
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
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param desc Row descriptor.
     * @param expCls Expected value class.
     * @param type Expected column type to convert to.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    private static Object convert(Object val, GridH2RowDescriptor desc, Class<?> expCls, int type)
        throws IgniteCheckedException {
        if (val == null)
            return null;

        Class<?> currCls = val.getClass();

        try {
            if (val instanceof Date && currCls != Date.class && expCls == Date.class) {
                // H2 thinks that java.util.Date is always a Timestamp, while binary marshaller expects
                // precise Date instance. Let's satisfy it.
                return new Date(((Date) val).getTime());
            }

            // User-given UUID is always serialized by H2 to byte array, so we have to deserialize manually
            if (type == Value.UUID && currCls == byte[].class)
                return U.unmarshal(desc.context().marshaller(), (byte[]) val,
                    U.resolveClassLoader(desc.context().gridConfig()));

            if (LocalDateTimeUtils.isJava8DateApiPresent()) {
                if (val instanceof Timestamp && LocalDateTimeUtils.isLocalDateTime(expCls))
                    return LocalDateTimeUtils.valueToLocalDateTime(ValueTimestamp.get((Timestamp) val));

                if (val instanceof Date && LocalDateTimeUtils.isLocalDate(expCls))
                    return LocalDateTimeUtils.valueToLocalDate(ValueDate.fromDateValue(
                        DateTimeUtils.dateValueFromDate(((Date) val).getTime())));

                if (val instanceof Time && LocalDateTimeUtils.isLocalTime(expCls))
                    return LocalDateTimeUtils.valueToLocalTime(ValueTime.get((Time) val));
            }

            // We have to convert arrays of reference types manually -
            // see https://issues.apache.org/jira/browse/IGNITE-4327
            // Still, we only can convert from Object[] to something more precise.
            if (type == Value.ARRAY && currCls != expCls) {
                if (currCls != Object[].class)
                    throw new IgniteCheckedException("Unexpected array type - only conversion from Object[] " +
                        "is assumed");

                // Why would otherwise type be Value.ARRAY?
                assert expCls.isArray();

                Object[] curr = (Object[]) val;

                Object newArr = Array.newInstance(expCls.getComponentType(), curr.length);

                System.arraycopy(curr, 0, newArr, 0, curr.length);

                return newArr;
            }

            return H2Utils.convert(val, desc, type);
        }
        catch (Exception e) {
            throw new IgniteSQLException("Value conversion failed [from=" + currCls.getName() + ", to=" +
                expCls.getName() +']', IgniteQueryErrorCode.CONVERSION_FAILED, e);
        }
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private static PageProcessingErrorResult splitErrors(Map<Object, EntryProcessorResult<Boolean>> res) {
        Set<Object> errKeys = new LinkedHashSet<>(res.keySet());

        SQLException currSqlEx = null;

        SQLException firstSqlEx = null;

        int errors = 0;

        // Let's form a chain of SQL exceptions
        for (Map.Entry<Object, EntryProcessorResult<Boolean>> e : res.entrySet()) {
            try {
                e.getValue().get();
            }
            catch (EntryProcessorException ex) {
                SQLException next = createJdbcSqlException("Failed to process key '" + e.getKey() + '\'',
                    IgniteQueryErrorCode.ENTRY_PROCESSING);

                next.initCause(ex);

                if (currSqlEx != null)
                    currSqlEx.setNextException(next);
                else
                    firstSqlEx = next;

                currSqlEx = next;

                errKeys.remove(e.getKey());

                errors++;
            }
        }

        return new PageProcessingErrorResult(errKeys.toArray(), firstSqlEx, errors);
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
        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        // If we have just one item to put, just do so
        if (plan.rowsNum == 1) {
            IgniteBiTuple t = rowToKeyValue(cctx, cursor.iterator().next(), plan);

            cctx.cache().put(t.getKey(), t.getValue());

            return 1;
        }
        else {
            int resCnt = 0;

            Map<Object, Object> rows = new LinkedHashMap<>();

            for (Iterator<List<?>> it = cursor.iterator(); it.hasNext();) {
                List<?> row = it.next();

                IgniteBiTuple t = rowToKeyValue(cctx, row, plan);

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
        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        // If we have just one item to put, just do so
        if (plan.rowsNum == 1) {
            IgniteBiTuple t = rowToKeyValue(cctx, cursor.iterator().next(), plan);

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw new IgniteSQLException("Duplicate key during INSERT [key=" + t.getKey() + ']',
                    DUPLICATE_KEY);
        }
        else {
            // Keys that failed to INSERT due to duplication.
            BatchSender sender = new BatchSender(cctx, pageSize);

            for (List<?> row : cursor) {
                final IgniteBiTuple keyValPair = rowToKeyValue(cctx, row, plan);

                sender.add(keyValPair.getKey(), new InsertEntryProcessor(keyValPair.getValue()));
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
     * Execute given entry processors and collect errors, if any.
     * @param cctx Cache context.
     * @param rows Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static PageProcessingResult processPage(GridCacheContext cctx,
        Map<Object, EntryProcessor<Object, Object, Boolean>> rows) throws IgniteCheckedException {
        Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows);

        if (F.isEmpty(res))
            return new PageProcessingResult(rows.size(), null, null);

        PageProcessingErrorResult splitRes = splitErrors(res);

        int keysCnt = splitRes.errKeys.length;

        return new PageProcessingResult(rows.size() - keysCnt - splitRes.cnt, splitRes.errKeys, splitRes.ex);
    }

    /**
     * Convert row presented as an array of Objects into key-value pair to be inserted to cache.
     * @param cctx Cache context.
     * @param row Row to process.
     * @param plan Update plan.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ResultOfMethodCallIgnored"})
    private IgniteBiTuple<?, ?> rowToKeyValue(GridCacheContext cctx, List<?> row, UpdatePlan plan)
        throws IgniteCheckedException {
        GridH2RowDescriptor rowDesc = plan.tbl.rowDescriptor();
        GridQueryTypeDescriptor desc = rowDesc.type();

        Object key = plan.keySupplier.apply(row);

        if (QueryUtils.isSqlType(desc.keyClass())) {
            assert plan.keyColIdx != -1;

            key = convert(key, rowDesc, desc.keyClass(), plan.colTypes[plan.keyColIdx]);
        }

        Object val = plan.valSupplier.apply(row);

        if (QueryUtils.isSqlType(desc.valueClass())) {
            assert plan.valColIdx != -1;

            val = convert(val, rowDesc, desc.valueClass(), plan.colTypes[plan.valColIdx]);
        }

        if (key == null) {
            if (F.isEmpty(desc.keyFieldName()))
                throw new IgniteSQLException("Key for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_KEY);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.keyFieldName() + "'",
                    IgniteQueryErrorCode.NULL_KEY);
        }

        if (val == null) {
            if (F.isEmpty(desc.valueFieldName()))
                throw new IgniteSQLException("Value for INSERT, MERGE, or UPDATE must not be null",
                    IgniteQueryErrorCode.NULL_VALUE);
            else
                throw new IgniteSQLException("Null value is not allowed for column '" + desc.valueFieldName() + "'",
                    IgniteQueryErrorCode.NULL_VALUE);
        }

        Map<String, Object> newColVals = new HashMap<>();

        for (int i = 0; i < plan.colNames.length; i++) {
            if (i == plan.keyColIdx || i == plan.valColIdx)
                continue;

            String colName = plan.colNames[i];

            GridQueryProperty prop = desc.property(colName);

            assert prop != null;

            Class<?> expCls = prop.type();

            newColVals.put(colName, convert(row.get(i), rowDesc, expCls, plan.colTypes[i]));
        }

        // We update columns in the order specified by the table for a reason - table's
        // column order preserves their precedence for correct update of nested properties.
        Column[] cols = plan.tbl.getColumns();

        // First 3 columns are _key, _val and _ver. Skip 'em.
        for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
            if (plan.tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                continue;

            String colName = cols[i].getName();

            if (!newColVals.containsKey(colName))
                continue;

            Object colVal = newColVals.get(colName);

            desc.setValue(colName, key, val, colVal);
        }

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder) key).build();

            if (val instanceof BinaryObjectBuilder)
                val = ((BinaryObjectBuilder) val).build();
        }

        desc.validateKeyAndValue(key, val);

        return new IgniteBiTuple<>(key, val);
    }

    /**
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
     * @param mvccVer Mvcc version.
     * @return Iterator upon updated values.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<?> prepareDistributedUpdate(String schema, Connection conn,
        PreparedStatement stmt, SqlFieldsQuery qry,
        IndexingQueryFilter filter, GridQueryCancel cancel, boolean local,
        AffinityTopologyVersion topVer, MvccCoordinatorVersion mvccVer) throws IgniteCheckedException {

        Prepared prepared = GridSqlQueryParser.prepared(stmt);

        UpdatePlan plan = getPlanForStatement(schema, conn, prepared, qry, local, null);

        GridCacheContext cctx = plan.tbl.cache();

        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null, false);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!local && !plan.isLocSubqry) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQry, qry.isCollocated())
                .setArgs(qry.getArgs())
                .setDistributedJoins(qry.isDistributedJoins())
                .setEnforceJoinOrder(qry.isEnforceJoinOrder())
                .setLocal(qry.isLocal())
                .setPageSize(qry.getPageSize())
                .setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>)idx.queryDistributedSqlFields(schema, newFieldsQry, true,
                cancel, cctx.cacheId(), true).get(0);
        }
        else {
            final GridQueryFieldsResult res = idx.queryLocalSqlFields(schema, plan.selectQry,
                F.asList(qry.getArgs()), filter, qry.isEnforceJoinOrder(), qry.getTimeout(), cancel, new MvccQueryTracker(cctx, mvccVer, topVer));

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

        switch (plan.mode) {
            case INSERT:
                return new InsertIterator(cur, plan, topVer);
            case UPDATE:
                return new UpdateIterator(cur, plan, topVer);
            case DELETE:
                return new DeleteIterator(cur, plan, topVer);

            default:
                throw new UnsupportedOperationException(String.valueOf(plan.mode));
        }
    }

    /** */
    private abstract static class AbstractIterator extends GridCloseableIteratorAdapter<Object> {
        /** */
        protected final QueryCursor<List<?>> cur;

        /** */
        protected final UpdatePlan plan;

        /** */
        protected final Iterator<List<?>> it;

        /** */
        protected final GridCacheContext cctx;

        /** */
        protected final AffinityTopologyVersion topVer;

        /** */
        protected final GridCacheAffinityManager affinity;

        /** */
        protected Object curr;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private AbstractIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            this.cur = cur;
            this.plan = plan;
            this.topVer = topVer;

            it = cur.iterator();
            cctx = plan.tbl.cache();
            affinity = cctx.affinity();
        }

        /** {@inheritDoc} */
        @Override protected Object onNext() throws IgniteCheckedException {
            advance();

            if(curr == null)
                throw new NoSuchElementException();

            Object res = curr;

            curr = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            advance();

            return curr != null;
        }

        /** */
        protected abstract void advance() throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override protected void onClose() {
            cur.close();
        }
    }

    /** */
    private final class UpdateIterator extends AbstractIterator {
        /** */
        private final boolean bin;

        /** */
        private final GridH2RowDescriptor desc;

        /** */
        private final boolean hasNewVal;

        /** */
        private final boolean hasProps;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private UpdateIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);

            bin = cctx.binaryMarshaller();
            desc = plan.tbl.rowDescriptor();

            hasNewVal = (plan.valColIdx != -1);
            hasProps = !hasNewVal || plan.colNames.length > 1;
        }

        /** {@inheritDoc} */
        @Override protected void advance() throws IgniteCheckedException {
            if(curr != null)
                return;

            if (it.hasNext()) {
                List<?> row = it.next();

                Map<String, Object> newColVals = new HashMap<>();

                for (int i = 0; i < plan.colNames.length; i++) {
                    if (hasNewVal && i == plan.valColIdx - 2)
                        continue;

                    GridQueryProperty prop = plan.tbl.rowDescriptor().type().property(plan.colNames[i]);

                    assert prop != null : "Unknown property: " + plan.colNames[i];

                    newColVals.put(plan.colNames[i], convert(row.get(i + 2), desc, prop.type(), plan.colTypes[i]));
                }

                Object newVal = plan.valSupplier.apply(row);

                if (newVal == null)
                    throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

                // Skip key and value - that's why we start off with 3rd column
                for (int i = 0; i < plan.tbl.getColumns().length - DEFAULT_COLUMNS_COUNT; i++) {
                    Column c = plan.tbl.getColumn(i + DEFAULT_COLUMNS_COUNT);

                    if (desc.isKeyValueOrVersionColumn(c.getColumnId()))
                        continue;

                    GridQueryProperty prop = desc.type().property(c.getName());

                    if (prop.key())
                        continue; // Don't get values of key's columns - we won't use them anyway

                    boolean hasNewColVal = newColVals.containsKey(c.getName());

                    if (!hasNewColVal)
                        continue;

                    Object colVal = newColVals.get(c.getName());

                    // UPDATE currently does not allow to modify key or its fields, so we must be safe to pass null as key.
                    desc.setColumnValue(null, newVal, colVal, i);
                }

                if (bin && hasProps) {
                    assert newVal instanceof BinaryObjectBuilder;

                    newVal = ((BinaryObjectBuilder)newVal).build();
                }

                desc.type().validateKeyAndValue(row.get(0), newVal);

                curr = new Object[] {row.get(0), newVal};
            }
        }
    }

    /** */
    private final class DeleteIterator extends AbstractIterator {

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private DeleteIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);
        }

        /** {@inheritDoc} */
        @Override protected void advance() {
            if(curr != null)
                return;

            if (it.hasNext())
                curr = it.next().get(0);
        }
    }

    /** */
    private final class InsertIterator extends AbstractIterator {
        /** */
        private final GridH2RowDescriptor rowDesc;

        /** */
        private final GridQueryTypeDescriptor desc;

        /**
         * @param cur Query cursor.
         * @param plan Update plan.
         * @param topVer Topology version.
         */
        private InsertIterator(QueryCursor<List<?>> cur, UpdatePlan plan, AffinityTopologyVersion topVer) {
            super(cur, plan, topVer);

            rowDesc = plan.tbl.rowDescriptor();
            desc = rowDesc.type();
        }

        /** {@inheritDoc} */
        @Override protected void advance() throws IgniteCheckedException {
            if(curr != null)
                return;

            while (it.hasNext()) {
                List<?> row = it.next();

                Object key = plan.keySupplier.apply(row);

                if (QueryUtils.isSqlType(desc.keyClass())) {
                    assert plan.keyColIdx != -1;

                    key = convert(key, rowDesc, desc.keyClass(), plan.colTypes[plan.keyColIdx]);
                }

                if (key == null) {
                    if (F.isEmpty(desc.keyFieldName()))
                        throw new IgniteSQLException("Key for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_KEY);
                    else
                        throw new IgniteSQLException("Null value is not allowed for column '" + desc.keyFieldName() + "'",
                            IgniteQueryErrorCode.NULL_KEY);
                }

                if (affinity.primaryByKey(cctx.localNode(), key, topVer)) {
                    Object val = plan.valSupplier.apply(row);

                    if (QueryUtils.isSqlType(desc.valueClass())) {
                        assert plan.valColIdx != -1;

                        val = convert(val, rowDesc, desc.valueClass(), plan.colTypes[plan.valColIdx]);
                    }

                    if (val == null) {
                        if (F.isEmpty(desc.valueFieldName()))
                            throw new IgniteSQLException("Value for INSERT, MERGE, or UPDATE must not be null",
                                IgniteQueryErrorCode.NULL_VALUE);
                        else
                            throw new IgniteSQLException("Null value is not allowed for column '" + desc.valueFieldName() + "'",
                                IgniteQueryErrorCode.NULL_VALUE);
                    }

                    Map<String, Object> newColVals = new HashMap<>();

                    for (int i = 0; i < plan.colNames.length; i++) {
                        if (i == plan.keyColIdx || i == plan.valColIdx)
                            continue;

                        String colName = plan.colNames[i];

                        GridQueryProperty prop = desc.property(colName);

                        assert prop != null;

                        Class<?> expCls = prop.type();

                        newColVals.put(colName, convert(row.get(i), rowDesc, expCls, plan.colTypes[i]));
                    }

                    // We update columns in the order specified by the table for a reason - table's
                    // column order preserves their precedence for correct update of nested properties.
                    Column[] cols = plan.tbl.getColumns();

                    // First 3 columns are _key, _val and _ver. Skip 'em.
                    for (int i = DEFAULT_COLUMNS_COUNT; i < cols.length; i++) {
                        if (plan.tbl.rowDescriptor().isKeyValueOrVersionColumn(i))
                            continue;

                        String colName = cols[i].getName();

                        if (!newColVals.containsKey(colName))
                            continue;

                        Object colVal = newColVals.get(colName);

                        desc.setValue(colName, key, val, colVal);
                    }

                    if (cctx.binaryMarshaller()) {
                        if (key instanceof BinaryObjectBuilder)
                            key = ((BinaryObjectBuilder) key).build();

                        if (val instanceof BinaryObjectBuilder)
                            val = ((BinaryObjectBuilder) val).build();
                    }

                    desc.validateKeyAndValue(key, val);

                    curr = new Object[] {key, null, new InsertEntryProcessor0(val), null};

                    return;
                }
            }
        }
    }

    /** */
    private static final class InsertEntryProcessor0 implements EntryProcessor<Object, Object, Void> {
        /** */
        private final Object val;

        /**
         * @param val Value to insert.
         */
        private InsertEntryProcessor0(Object val) {
            this.val = val;
        }

        @Override public Void process(MutableEntry<Object, Object> entry, Object... args) throws EntryProcessorException {
            if (entry.exists())
                throw new IgniteSQLException("Duplicate key during INSERT [key=" +
                    entry.getKey() + ']', DUPLICATE_KEY);

            entry.setValue(val);

            return null;
        }
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
     * Check update result for erroneous keys and throws concurrent update exception if necessary.
     *
     * @param r Update result.
     */
    static void checkUpdateResult(UpdateResult r) {
        if (!F.isEmpty(r.errorKeys())) {
            String msg = "Failed to update some keys because they had been modified concurrently " +
                "[keys=" + r.errorKeys() + ']';

            SQLException conEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

            throw new IgniteSQLException(conEx);
        }
    }

    /** Result of processing an individual page with {@link IgniteCache#invokeAll} including error details, if any. */
    private final static class PageProcessingResult {
        /** Number of successfully processed items. */
        final long cnt;

        /** Keys that failed to be updated or deleted due to concurrent modification of values. */
        @NotNull
        final Object[] errKeys;

        /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
        final SQLException ex;

        /** */
        @SuppressWarnings("ConstantConditions")
        private PageProcessingResult(long cnt, Object[] errKeys, SQLException ex) {
            this.cnt = cnt;
            this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
            this.ex = ex;
        }
    }

    /** Result of splitting keys whose processing resulted into an exception from those skipped by
     * logic of {@link EntryProcessor}s (most likely INSERT duplicates, or UPDATE/DELETE keys whose values
     * had been modified concurrently), counting and collecting entry processor exceptions.
     */
    private final static class PageProcessingErrorResult {
        /** Keys that failed to be processed by {@link EntryProcessor} (not due to an exception). */
        @NotNull
        final Object[] errKeys;

        /** Number of entries whose processing resulted into an exception. */
        final int cnt;

        /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
        final SQLException ex;

        /** */
        @SuppressWarnings("ConstantConditions")
        private PageProcessingErrorResult(@NotNull Object[] errKeys, SQLException ex, int exCnt) {
            errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
            // When exceptions count must be zero, exceptions chain must be not null, and vice versa.
            assert exCnt == 0 ^ ex != null;

            this.errKeys = errKeys;
            this.cnt = exCnt;
            this.ex = ex;
        }
    }

    /**
     * Batch sender class.
     */
    private static class BatchSender {
        /** Cache context. */
        private final GridCacheContext cctx;

        /** Batch size. */
        private final int size;

        /** Batches. */
        private final Map<UUID, Map<Object, EntryProcessor<Object, Object, Boolean>>> batches = new HashMap<>();

        /** Result count. */
        private long updateCnt;

        /** Failed keys. */
        private List<Object> failedKeys;

        /** Exception. */
        private SQLException err;

        /**
         * Constructor.
         *
         * @param cctx Cache context.
         * @param size Batch.
         */
        public BatchSender(GridCacheContext cctx, int size) {
            this.cctx = cctx;
            this.size = size;
        }

        /**
         * Add entry to batch.
         *
         * @param key Key.
         * @param proc Processor.
         */
        public void add(Object key, EntryProcessor<Object, Object, Boolean> proc) throws IgniteCheckedException {
            ClusterNode node = cctx.affinity().primaryByKey(key, AffinityTopologyVersion.NONE);

            if (node == null)
                throw new IgniteCheckedException("Failed to map key to node.");

            UUID nodeId = node.id();

            Map<Object, EntryProcessor<Object, Object, Boolean>> batch = batches.get(nodeId);

            if (batch == null) {
                batch = new HashMap<>();

                batches.put(nodeId, batch);
            }

            batch.put(key, proc);

            if (batch.size() >= size) {
                sendBatch(batch);

                batch.clear();
            }
        }

        /**
         * Flush any remaining entries.
         *
         * @throws IgniteCheckedException If failed.
         */
        public void flush() throws IgniteCheckedException {
            for (Map<Object, EntryProcessor<Object, Object, Boolean>> batch : batches.values()) {
                if (!batch.isEmpty())
                    sendBatch(batch);
            }
        }

        /**
         * @return Update count.
         */
        public long updateCount() {
            return updateCnt;
        }

        /**
         * @return Failed keys.
         */
        public List<Object> failedKeys() {
            return failedKeys != null ? failedKeys : Collections.emptyList();
        }

        /**
         * @return Error.
         */
        public SQLException error() {
            return err;
        }

        /**
         * Send the batch.
         *
         * @param batch Batch.
         * @throws IgniteCheckedException If failed.
         */
        private void sendBatch(Map<Object, EntryProcessor<Object, Object, Boolean>> batch)
            throws IgniteCheckedException {
            PageProcessingResult pageRes = processPage(cctx, batch);

            updateCnt += pageRes.cnt;

            if (failedKeys == null)
                failedKeys = new ArrayList<>();

            failedKeys.addAll(F.asList(pageRes.errKeys));

            if (pageRes.ex != null) {
                if (err == null)
                    err = pageRes.ex;
                else
                    err.setNextException(pageRes.ex);
            }
        }
    }
}
