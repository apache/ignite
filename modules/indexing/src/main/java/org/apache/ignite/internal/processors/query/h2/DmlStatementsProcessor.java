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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.DmlAstUtils;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridPlainClosure;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.table.Column;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.createSqlException;

/**
 *
 */
class DmlStatementsProcessor {
    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private final static int DFLT_DML_RERUN_ATTEMPTS = 4;

    /** Indexing. */
    private final IgniteH2Indexing indexing;

    /** Set of binary type ids for which warning about missing identity in configuration has been printed. */
    private final static Set<Integer> WARNED_TYPES =
        Collections.newSetFromMap(new ConcurrentHashMap8<Integer, Boolean>());

    /** Default size for update plan cache. */
    private static final int PLAN_CACHE_SIZE = 1024;

    /** Update plans cache. */
    private final ConcurrentMap<String, UpdatePlan> planCache = new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

    /** Dummy metadata for update result. */
    private final static List<GridQueryFieldMetadata> UPDATE_RESULT_META = Collections.<GridQueryFieldMetadata>
        singletonList(new IgniteH2Indexing.SqlFieldMetadata(null, null, "count", Long.class.getName()));

    /**
     * @param indexing indexing.
     */
    DmlStatementsProcessor(IgniteH2Indexing indexing) {
        this.indexing = indexing;
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param cctx Cache context.
     * @param stmt JDBC statement.
     * @param fieldsQry Original query.
     * @param loc Query locality flag.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private long updateSqlFields(GridCacheContext cctx, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, boolean loc, GridQueryCancel cancel) throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        UpdatePlan plan = getPlanForStatement(stmt, null);

        // Let's verify that user does not try to mess with key's columns directly
        verifyUpdateColumns(plan.initStmt);

        GridSqlTable tbl = gridTableForElement(plan.target);

        // Switch to cache specified in query.
        cctx = cctx.shared().cacheContext(CU.cacheId(tbl.schema()));

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            UpdateResult r = executeUpdateStatement(cctx, stmt, fieldsQry, loc, cancel, errKeys);

            if (F.isEmpty(r.errKeys))
                return r.cnt + items;
            else {
                items += r.cnt;
                errKeys = r.errKeys;
            }
        }

        throw createSqlException("Failed to update or delete some keys: " + Arrays.deepToString(errKeys),
            ErrorCode.CONCURRENT_UPDATE_1);
    }

    /**
     * @param cctx Cache context.
     * @param stmt Prepared statement.
     * @param fieldsQry Initial query.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    QueryCursorImpl<List<?>> updateSqlFieldsTwoStep(GridCacheContext cctx, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry) throws IgniteCheckedException {
        long res = updateSqlFields(cctx, stmt, fieldsQry, false, null);

        return cursorForUpdateResult(res);
    }

    /**
     * @param cctx Cache context.
     * @param stmt Prepared statement.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateLocalSqlFields(GridCacheContext cctx, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, GridQueryCancel cancel) throws IgniteCheckedException {
        long res = updateSqlFields(cctx, stmt, fieldsQry, true, cancel);

        return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META,
            new IgniteSingletonIterator(Collections.singletonList(res)));
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param cctx Cache context.
     * @param prepStmt Prepared statement for DML query.
     * @param failedKeys Keys to restrict UPDATE and DELETE operations with. Null or empty array means no restriction.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private UpdateResult executeUpdateStatement(GridCacheContext cctx, PreparedStatement prepStmt,
        SqlFieldsQuery fieldsQry, boolean loc, GridQueryCancel cancel, Object[] failedKeys) throws IgniteCheckedException {
        Integer errKeysPos = null;

        if (!F.isEmpty(failedKeys))
            errKeysPos = F.isEmpty(fieldsQry.getArgs()) ? 1 : fieldsQry.getArgs().length + 1;

        UpdatePlan plan = getPlanForStatement(prepStmt, errKeysPos);

        GridSqlStatement stmt = plan.initStmt;

        Object[] params = fieldsQry.getArgs();

        if (plan.singleUpdate != null) {
            assert F.isEmpty(failedKeys) && errKeysPos == null;

            return new UpdateResult(doSingleUpdate(cctx, plan.singleUpdate, stmt, params), X.EMPTY_OBJECT_ARRAY);
        }

        assert plan.selectQry != null;

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // subquery and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && plan.isSubqry) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQry.getSQL(), fieldsQry.isCollocated())
                .setArgs(params)
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>) indexing.queryTwoStep(cctx, newFieldsQry);
        }
        else {
            Connection conn = indexing.connectionForSpace(cctx.name());

            ResultSet rs = indexing.executeSqlQueryWithTimer(cctx.name(), conn, plan.selectQry.getSQL(), F.asList(params),
                true, fieldsQry.getTimeout(), cancel);

            final Iterator<List<?>> rsIter = new IgniteH2Indexing.FieldsIterator(rs);

            Iterable<List<?>> it = new Iterable<List<?>>() {
                /**
                 * {@inheritDoc}
                 */
                @Override public Iterator<List<?>> iterator() {
                    return rsIter;
                }
            };

            cur = new QueryCursorImpl<>(it);
        }

        int pageSize = loc ? 0 : fieldsQry.getPageSize();

        if (stmt instanceof GridSqlMerge)
            return new UpdateResult(doMerge(cctx, (GridSqlMerge) stmt, cur, pageSize), X.EMPTY_OBJECT_ARRAY);
        else if (stmt instanceof GridSqlInsert)
            return new UpdateResult(doInsert(cctx, (GridSqlInsert) stmt, cur, pageSize), X.EMPTY_OBJECT_ARRAY);
        else if (stmt instanceof GridSqlUpdate)
            return doUpdate(cctx, (GridSqlUpdate) stmt, cur, pageSize);
        else if (stmt instanceof GridSqlDelete)
            return doDelete(cctx, cur, pageSize);
        else
            throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                ErrorCode.UNKNOWN_MODE_1);
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param prepStmt JDBC statement.
     * @return Update plan.
     */
    private UpdatePlan getPlanForStatement(PreparedStatement prepStmt, @Nullable Integer errKeysPos) {
        Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement) prepStmt);

        assert !p.isQuery();

        // getSQL returns field value, so it's fast
        // Don't look for re-runs in cache, we don't cache them
        UpdatePlan res = (errKeysPos == null ? planCache.get(p.getSQL()) : null);

        if (res != null)
            return res;

        GridSqlStatement stmt = new GridSqlQueryParser().parse(p);

        GridSqlElement target;

        if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert) {
            GridSqlQuery sel;

            boolean isSubqry;

            if (stmt instanceof GridSqlInsert) {
                GridSqlInsert ins = (GridSqlInsert) stmt;
                target = ins.into();
                sel = DmlAstUtils.selectForInsertOrMerge(ins.rows(), ins.query());
                isSubqry = (ins.query() != null);
            } else {
                GridSqlMerge merge = (GridSqlMerge) stmt;
                target = merge.into();
                sel = DmlAstUtils.selectForInsertOrMerge(merge.rows(), merge.query());
                isSubqry = (merge.query() != null);
            }

            // Let's set the flag only for subqueries that have their FROM specified.
            isSubqry = (isSubqry && sel instanceof GridSqlSelect && ((GridSqlSelect) sel).from() != null);

            res = new UpdatePlan(stmt, target, sel, isSubqry, null);
        }
        else {
            GridTriple<GridSqlElement> singleUpdate;

            if (stmt instanceof GridSqlUpdate) {
                GridSqlUpdate update = (GridSqlUpdate) stmt;
                target = update.target();
                singleUpdate = DmlAstUtils.getSingleItemFilter(update);
            }
            else if (stmt instanceof GridSqlDelete) {
                GridSqlDelete del = (GridSqlDelete) stmt;
                target = del.from();
                singleUpdate = DmlAstUtils.getSingleItemFilter(del);
            }
            else
                throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                    ErrorCode.UNKNOWN_MODE_1);

            if (singleUpdate != null)
                res = new UpdatePlan(stmt, target, null, false, singleUpdate);
            else {
                GridSqlSelect sel;

                if (stmt instanceof GridSqlUpdate)
                    sel = DmlAstUtils.mapQueryForUpdate((GridSqlUpdate) stmt, errKeysPos);
                else
                    sel = DmlAstUtils.mapQueryForDelete((GridSqlDelete) stmt, errKeysPos);

                res = new UpdatePlan(stmt, target, sel, true, null);
            }
        }

        // Don't cache re-runs
        if (errKeysPos == null)
            return U.firstNotNull(planCache.putIfAbsent(p.getSQL(), res), res);
        else
            return res;
    }

    /**
     * Perform single cache operation based on given args.
     *
     * @param cctx Cache context.
     * @param singleUpdate Triple {@code [key; value; new value]} to perform remove or replace with.
     * @param stmt Statement.
     * @param params Query parameters.
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private static long doSingleUpdate(GridCacheContext cctx, GridTriple<GridSqlElement> singleUpdate,
        GridSqlStatement stmt, Object[] params) throws IgniteCheckedException {
        GridSqlElement target;

        if (stmt instanceof GridSqlUpdate)
            target = ((GridSqlUpdate) stmt).target();
        else if (stmt instanceof GridSqlDelete)
            target = ((GridSqlDelete) stmt).from();
        else
            throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                ErrorCode.UNKNOWN_MODE_1);

        GridSqlTable tbl = gridTableForElement(target);

        IgniteCache cache = cctx.grid().cache(tbl.schema());

        int res;

        Object key = getElementValue(singleUpdate.get1(), params);
        Object val = getElementValue(singleUpdate.get2(), params);
        Object newVal = getElementValue(singleUpdate.get3(), params);

        if (newVal != null) { // Single item UPDATE
            if (val == null) // No _val bound in source query
                res = cache.replace(key, newVal) ? 1 : 0;
            else
                res = cache.replace(key, val, newVal) ? 1 : 0;
        }
        else { // Single item DELETE
            if (val == null) // No _val bound in source query
                res = cache.remove(key) ? 1 : 0;
            else
                res = cache.remove(key, val) ? 1 : 0;
        }

        return res;
    }

    /**
     * Perform DELETE operation on top of results of SELECT.
     * @param cctx Cache context.
     * @param cursor SELECT results.
     * @param pageSize Page size for streaming, anything <= 0 for single batch operations.
     * @return Results of DELETE (number of items affected AND keys that failed to be updated).
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private UpdateResult doDelete(GridCacheContext cctx, QueryCursorImpl<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        // With DELETE, we have only two columns - key and value.
        long res = 0;

        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        // Keys that failed to DELETE due to concurrent updates.
        List<Object> failedKeys = new ArrayList<>();

        SQLException resEx = null;

        try {
            Iterator<List<?>> it = cursor.iterator();
            Map<Object, EntryProcessor<Object, Object, Boolean>> rows = new LinkedHashMap<>();

            while (it.hasNext()) {
                List<?> e = it.next();
                if (e.size() != 2) {
                    U.warn(indexing.getLogger(), "Invalid row size on DELETE - expected 2, got " + e.size());
                    continue;
                }

                rows.put(e.get(0), new ModifyingEntryProcessor(e.get(1), RMV));

                if ((pageSize > 0 && rows.size() == pageSize) || (!it.hasNext())) {
                    BatchProcessingResult batchRes = processBatch(cctx, rows);

                    res += batchRes.cnt;

                    failedKeys.addAll(F.asList(batchRes.errKeys));

                    if (batchRes.ex != null) {
                        if (resEx == null)
                            resEx = batchRes.ex;
                        else
                            resEx.setNextException(batchRes.ex);
                    }

                    if (it.hasNext())
                        rows.clear(); // No need to clear after the last batch.
                }
            }

            if (resEx != null) {
                if (!F.isEmpty(failedKeys)) {
                    // Don't go for a re-run if processing of some keys yielded exceptions and report keys that
                    // had been modified concurrently right away.
                    String msg = "Failed to DELETE some keys because they had been modified concurrently " +
                        "[keys=" + failedKeys + ']';

                    SQLException conEx = new SQLException(msg, ErrorCode.getState(ErrorCode.CONCURRENT_UPDATE_1),
                        ErrorCode.CONCURRENT_UPDATE_1);

                    conEx.setNextException(resEx);

                    resEx = conEx;
                }

                throw new IgniteSQLException(resEx);
            }
        }
        finally {
            cctx.operationContextPerCall(opCtx);
        }

        return new UpdateResult(res, failedKeys.toArray());
    }

    /**
     * Perform UPDATE operation on top of results of SELECT.
     * @param cctx Cache context.
     * @param update Update statement.
     * @param cursor SELECT results.
     * @param pageSize Batch size for streaming, anything <= 0 for single batch operations.
     * @return Pair [cursor corresponding to results of UPDATE (contains number of items affected); keys whose values
     *     had been modified concurrently (arguments for a re-run)].
     */
    @SuppressWarnings("unchecked")
    private UpdateResult doUpdate(GridCacheContext cctx, GridSqlUpdate update,
        QueryCursorImpl<List<?>> cursor, int pageSize) throws IgniteCheckedException {

        GridSqlTable tbl = gridTableForElement(update.target());

        GridH2Table gridTbl = tbl.dataTable();

        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        if (desc == null)
            throw createSqlException("Row descriptor undefined for table '" + gridTbl.getName() + "'",
                ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        boolean bin = cctx.binaryMarshaller();

        Column[] cols = gridTbl.getColumns();

        List<GridSqlColumn> updatedCols = update.cols();

        int valColIdx = -1;

        for (int i = 0; i < updatedCols.size(); i++) {
            if (VAL_FIELD_NAME.equalsIgnoreCase(updatedCols.get(i).columnName())) {
                valColIdx = i;
                break;
            }
        }

        boolean hasNewVal = (valColIdx != -1);

        // Statement updates distinct properties if it does not have _val in updated columns list
        // or if its list of updated columns includes only _val, i.e. is single element.
        boolean hasProps = !hasNewVal || updatedCols.size() > 1;

        // Index of new _val in results of SELECT
        if (hasNewVal)
            valColIdx += 2;

        int newValColIdx;

        if (!hasProps) // No distinct properties, only whole new value - let's take it
            newValColIdx = valColIdx;
        else if (bin) // We update distinct columns in binary mode - let's choose correct index for the builder
            newValColIdx = (hasNewVal ? valColIdx : 1);
        else // Distinct properties, non binary mode - let's instantiate.
            newValColIdx = -1;

        // We want supplier to take present value only in case of binary mode as it will
        // otherwise we always want it to instantiate new object
        Supplier newValSupplier = createSupplier(cctx, desc.type(), newValColIdx, hasProps, false);

        long res = 0;

        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        Map<Object, EntryProcessor<Object, Object, Boolean>> rows = new LinkedHashMap<>();

        // Keys that failed to UPDATE due to concurrent updates.
        List<Object> failedKeys = new ArrayList<>();

        SQLException resEx = null;

        try {
            Iterator<List<?>> it = cursor.iterator();

            while (it.hasNext()) {
                List<?> e = it.next();
                Object key = e.get(0);
                Object val = (hasNewVal ? e.get(valColIdx) : e.get(1));

                Object newVal;

                Map<String, Object> newColVals = new HashMap<>();

                for (int i = 0; i < updatedCols.size(); i++) {
                    if (hasNewVal && i == valColIdx - 2)
                        continue;

                    newColVals.put(updatedCols.get(i).columnName(), e.get(i + 2));
                }

                newVal = newValSupplier.apply(e);

                if (bin && !(val instanceof BinaryObject))
                    val = cctx.grid().binary().toBinary(val);

                // Skip key and value - that's why we start off with 2nd column
                for (int i = 0; i < cols.length - 2; i++) {
                    Column c = cols[i + 2];

                    boolean hasNewColVal = newColVals.containsKey(c.getName());

                    // Binary objects get old field values from the Builder, so we can skip what we're not updating
                    if (bin && !hasNewColVal)
                        continue;

                    Object colVal = hasNewColVal ? newColVals.get(c.getName()) : desc.columnValue(key, val, i);

                    desc.setColumnValue(key, newVal, colVal, i);
                }

                if (bin && hasProps) {
                    assert newVal instanceof BinaryObjectBuilder;

                    newVal = ((BinaryObjectBuilder) newVal).build();
                }

                Object srcVal = e.get(1);

                if (bin && !(srcVal instanceof BinaryObject))
                    srcVal = cctx.grid().binary().toBinary(srcVal);

                rows.put(key, new ModifyingEntryProcessor(srcVal, new EntryValueUpdater(newVal)));

                if ((pageSize > 0 && rows.size() == pageSize) || (!it.hasNext())) {
                    BatchProcessingResult batchRes = processBatch(cctx, rows);

                    res += batchRes.cnt;

                    failedKeys.addAll(F.asList(batchRes.errKeys));

                    if (batchRes.ex != null) {
                        if (resEx == null)
                            resEx = batchRes.ex;
                        else
                            resEx.setNextException(batchRes.ex);
                    }

                    if (it.hasNext())
                        rows.clear(); // No need to clear after the last batch.
                }
            }

            if (resEx != null) {
                if (!F.isEmpty(failedKeys)) {
                    // Don't go for a re-run if processing of some keys yielded exceptions and report keys that
                    // had been modified concurrently right away.
                    String msg = "Failed to UPDATE some keys because they had been modified concurrently " +
                        "[keys=" + failedKeys + ']';

                    SQLException dupEx = new SQLException(msg, ErrorCode.getState(ErrorCode.DUPLICATE_KEY_1),
                        ErrorCode.DUPLICATE_KEY_1);

                    dupEx.setNextException(resEx);

                    resEx = dupEx;
                }

                throw new IgniteSQLException(resEx);
            }
        }
        finally {
            cctx.operationContextPerCall(opCtx);
        }

        return new UpdateResult(res, failedKeys.toArray());
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private static BatchProcessingErrorResult splitErrors(Map<Object, EntryProcessorResult<Boolean>> res) {
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
                SQLException next = new SQLException("Failed to process key '" + e.getKey() + '\'', ex);

                if (currSqlEx != null)
                    currSqlEx.setNextException(next);
                else
                    firstSqlEx = next;

                currSqlEx = next;

                errKeys.remove(e.getKey());

                errors++;
            }
        }

        return new BatchProcessingErrorResult(errKeys.toArray(), firstSqlEx, errors);
    }

    /**
     * @param cctx Cache context.
     * @param gridStmt Grid SQL statement.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size to stream data from {@code cursor}, anything <= 0 for single batch operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private long doMerge(GridCacheContext cctx, GridSqlMerge gridStmt, QueryCursorImpl<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        // This check also protects us from attempts to update key or its fields directly -
        // when no key except cache key can be used, it will serve only for uniqueness checks,
        // not for updates, and hence will allow putting new pairs only.
        GridSqlColumn[] keys = gridStmt.keys();
        if (keys.length != 1 || !keys[0].columnName().equalsIgnoreCase(KEY_FIELD_NAME))
            throw new CacheException("SQL MERGE does not support arbitrary keys");

        GridSqlColumn[] cols = gridStmt.columns();

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        GridSqlTable tbl = gridTableForElement(gridStmt.into());

        GridH2RowDescriptor desc = tbl.dataTable().rowDescriptor();

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].columnName().equalsIgnoreCase(KEY_FIELD_NAME)) {
                keyColIdx = i;
                continue;
            }

            if (cols[i].columnName().equalsIgnoreCase(VAL_FIELD_NAME)) {
                valColIdx = i;
                continue;
            }

            GridQueryProperty prop = desc.type().property(cols[i].columnName());

            assert prop != null : "Property '" + cols[i].columnName() + "' not found.";

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        Supplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true);
        Supplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false);

        Iterable<List<?>> src;

        boolean singleRow = false;

        if (gridStmt.query() != null)
            src = cursor;
        else {
            singleRow = gridStmt.rows().size() == 1;
            src = rowsCursorToRows(cursor);
        }

        // If we have just one item to put, just do so
        if (singleRow) {
            IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                src.iterator().next().toArray());
            cctx.cache().put(t.getKey(), t.getValue());
            return 1;
        }
        else {
            int resCnt = 0;
            Map<Object, Object> rows = new LinkedHashMap<>();

            for (Iterator<List<?>> it = src.iterator(); it.hasNext();) {
                List<?> row = it.next();

                IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                    row.toArray());
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
     * @param cctx Cache context.
     * @param ins Insert statement.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size for streaming, anything <= 0 for single batch operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private long doInsert(GridCacheContext cctx, GridSqlInsert ins, QueryCursorImpl<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        GridSqlColumn[] cols = ins.columns();

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        GridSqlTable tbl = gridTableForElement(ins.into());

        GridH2RowDescriptor desc = tbl.dataTable().rowDescriptor();

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].columnName().equalsIgnoreCase(KEY_FIELD_NAME)) {
                keyColIdx = i;
                continue;
            }

            if (cols[i].columnName().equalsIgnoreCase(VAL_FIELD_NAME)) {
                valColIdx = i;
                continue;
            }

            GridQueryProperty prop = desc.type().property(cols[i].columnName());

            assert prop != null : "Property '" + cols[i].columnName() + "' not found.";

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        Supplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true);
        Supplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false);

        Iterable<List<?>> src;

        boolean singleRow = false;

        if (ins.query() != null)
            src = cursor;
        else {
            singleRow = ins.rows().size() == 1;
            src = rowsCursorToRows(cursor);
        }

        // If we have just one item to put, just do so
        if (singleRow) {
            IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                src.iterator().next().toArray());

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw createSqlException("Duplicate key during INSERT [key=" + t.getKey() + ']',
                    ErrorCode.DUPLICATE_KEY_1);
        }
        else {
            CacheOperationContext opCtx = cctx.operationContextPerCall();

            // Force keepBinary for operation context to avoid binary deserialization inside entry processor
            if (cctx.binaryMarshaller()) {
                CacheOperationContext newOpCtx = null;

                if (opCtx == null)
                    // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                    newOpCtx = new CacheOperationContext(false, null, true, null, false, null);
                else if (!opCtx.isKeepBinary())
                    newOpCtx = opCtx.keepBinary();

                if (newOpCtx != null)
                    cctx.operationContextPerCall(newOpCtx);
            }

            Map<Object, EntryProcessor<Object, Object, Boolean>> rows = ins.query() == null ?
                new LinkedHashMap<Object, EntryProcessor<Object, Object, Boolean>>(ins.rows().size()) :
                new LinkedHashMap<Object, EntryProcessor<Object, Object, Boolean>>();

            // Keys that failed to INSERT due to duplication.
            List<Object> duplicateKeys = new ArrayList<>();

            int resCnt = 0;

            SQLException resEx = null;

            try {
                Iterator<List<?>> it = src.iterator();

                while (it.hasNext()) {
                    List<?> row = it.next();

                    final IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx,
                        cols, row.toArray());

                    rows.put(t.getKey(), new InsertEntryProcessor(t.getValue()));

                    if (!it.hasNext() || (pageSize > 0 && rows.size() == pageSize)) {
                        BatchProcessingResult batchRes = processBatch(cctx, rows);

                        resCnt += batchRes.cnt;

                        duplicateKeys.addAll(F.asList(batchRes.errKeys));

                        if (batchRes.ex != null) {
                            if (resEx == null)
                                resEx = batchRes.ex;
                            else
                                resEx.setNextException(batchRes.ex);
                        }

                        rows.clear();
                    }
                }

                if (!F.isEmpty(duplicateKeys)) {
                    String msg = "Failed to INSERT some keys because they are already in cache " +
                        "[keys=" + duplicateKeys + ']';

                    SQLException dupEx = new SQLException(msg, ErrorCode.getState(ErrorCode.DUPLICATE_KEY_1),
                        ErrorCode.DUPLICATE_KEY_1);

                    if (resEx == null)
                        resEx = dupEx;
                    else
                        resEx.setNextException(dupEx);
                }

                if (resEx != null)
                    throw new IgniteSQLException(resEx);

                return resCnt;
            }
            finally {
                cctx.operationContextPerCall(opCtx);
            }
        }
    }

    /**
     * @param cctx Cache context.
     * @param rows Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     * @throws IgniteCheckedException
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static BatchProcessingResult processBatch(GridCacheContext cctx,
        Map<Object, EntryProcessor<Object, Object, Boolean>> rows) throws IgniteCheckedException {

        Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows);

        if (F.isEmpty(res))
            return new BatchProcessingResult(rows.size(), null, null);

        BatchProcessingErrorResult splitRes = splitErrors(res);

        int keysCnt = splitRes.errKeys.length;

        return new BatchProcessingResult(rows.size() - keysCnt - splitRes.cnt, splitRes.errKeys, splitRes.ex);
    }

    /**
     * @param cursor single "row" cursor that has arrays as columns, each array represents new inserted row.
     * @return List of lists, each representing new inserted row.
     */
    private static List<List<?>> rowsCursorToRows(QueryCursorEx<List<?>> cursor) {
        List<List<?>> newRowsRow = cursor.getAll();

        // We expect all new rows to be selected as single row with "columns" each of which is an array
        assert newRowsRow.size() == 1;

        List<?> newRowsList = newRowsRow.get(0);

        List<List<?>> newRows = new ArrayList<>(newRowsList.size());

        for (Object o : newRowsList) {
            assert o instanceof Object[];

            newRows.add(F.asList((Object[]) o));
        }

        return newRows;
    }

    /**
     * Convert bunch of {@link GridSqlElement}s into key-value pair to be inserted to cache.
     *
     * @param cctx Cache context.
     * @param desc Table descriptor.
     * @param keySupplier Key instantiation method.
     * @param valSupplier Key instantiation method.
     * @param keyColIdx Key column index, or {@code -1} if no key column is mentioned in {@code cols}.
     * @param valColIdx Value column index, or {@code -1} if no value column is mentioned in {@code cols}.
     * @param cols Query cols.
     * @param row Row to process.
     * @return Key-value pair.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ResultOfMethodCallIgnored"})
    private IgniteBiTuple<?, ?> rowToKeyValue(GridCacheContext cctx, GridQueryTypeDescriptor desc, Supplier keySupplier,
                                              Supplier valSupplier, int keyColIdx, int valColIdx, GridSqlColumn[] cols, Object[] row)
        throws IgniteCheckedException {

        Object key = keySupplier.apply(F.asList(row));
        Object val = valSupplier.apply(F.asList(row));

        if (key == null)
            throw createSqlException("Key for INSERT or MERGE must not be null", ErrorCode.NULL_NOT_ALLOWED);

        if (val == null)
            throw createSqlException("Value for INSERT or MERGE must not be null", ErrorCode.NULL_NOT_ALLOWED);

        for (int i = 0; i < cols.length; i++) {
            if (i == keyColIdx || i == valColIdx)
                continue;

            desc.setValue(cols[i].columnName(), key, val, row[i]);
        }

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder) key).build();

            if (val instanceof BinaryObjectBuilder)
                val = ((BinaryObjectBuilder) val).build();

            if (key instanceof BinaryObject)
                key = updateHashCodeIfNeeded(cctx, (BinaryObject) key);

            if (val instanceof BinaryObject)
                val = updateHashCodeIfNeeded(cctx, (BinaryObject) val);
        }

        return new IgniteBiTuple<>(key, val);
    }

    /**
     * Set hash code to binary object if it does not have one.
     *
     * @param cctx Cache context.
     * @param binObj Binary object.
     * @return Binary object with hash code set.
     */
    private BinaryObject updateHashCodeIfNeeded(GridCacheContext cctx, BinaryObject binObj) {
        if (U.isHashCodeEmpty(binObj)) {
            if (WARNED_TYPES.add(binObj.type().typeId()))
                U.warn(indexing.getLogger(), "Binary object's type does not have identity resolver explicitly set, therefore " +
                    "BinaryArrayIdentityResolver is used to generate hash codes for its instances, and therefore " +
                    "hash code of this binary object will most likely not match that of its non serialized form. " +
                    "For finer control over identity of this type, please update your BinaryConfiguration accordingly." +
                    " [typeId=" + binObj.type().typeId() + ", typeName=" + binObj.type().typeName() + ']');

            int hash = BinaryArrayIdentityResolver.instance().hashCode(binObj);

            // Empty hash code means no identity set for the type, therefore, we can safely set hash code
            // via this Builder as it won't be overwritten.
            return cctx.grid().binary().builder(binObj)
                .hashCode(hash)
                .build();
        }
        else
            return binObj;
    }

    /**
     * Detect appropriate method of instantiating key or value (take from param, create binary builder,
     * invoke default ctor, or allocate).
     *
     * @param cctx Cache context.
     * @param desc Table descriptor.
     * @param colIdx Column index if key or value is present in columns list, {@code -1} if it's not.
     * @param hasProps Whether column list affects individual properties of key or value.
     * @param key Whether supplier should be created for key or for value.
     * @return Closure returning key or value.
     * @throws IgniteCheckedException
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private static Supplier createSupplier(final GridCacheContext<?, ?> cctx, GridQueryTypeDescriptor desc,
                                                            final int colIdx, boolean hasProps, final boolean key) throws IgniteCheckedException {
        final String typeName = key ? desc.keyTypeName() : desc.valueTypeName();

        //Try to find class for the key locally.
        final Class<?> cls = key ? U.firstNotNull(U.classForName(desc.keyTypeName(), null), desc.keyClass())
            : desc.valueClass();

        boolean isSqlType = GridQueryProcessor.isSqlType(cls);

        // If we don't need to construct anything from scratch, just return value from array.
        if (isSqlType || !hasProps || !cctx.binaryMarshaller()) {
            if (colIdx != -1)
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return arg.get(colIdx);
                    }
                };
            else if (isSqlType)
                // Non constructable keys and values (SQL types) must be present in the query explicitly.
                throw new IgniteCheckedException((key ? "Key" : "Value") + " is missing from query");
        }

        if (cctx.binaryMarshaller()) {
            if (colIdx != -1) {
                // If we have key or value explicitly present in query, create new builder upon them...
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        BinaryObject bin = cctx.grid().binary().toBinary(arg.get(colIdx));

                        return cctx.grid().binary().builder(bin);
                    }
                };
            }
            else {
                // ...and if we don't, just create a new builder.
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return cctx.grid().binary().builder(typeName);
                    }
                };
            }
        }
        else {
            Constructor<?> ctor;

            try {
                ctor = cls.getDeclaredConstructor();
                ctor.setAccessible(true);
            }
            catch (NoSuchMethodException | SecurityException ignored) {
                ctor = null;
            }

            if (ctor != null) {
                final Constructor<?> ctor0 = ctor;

                // Use default ctor, if it's present...
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return ctor0.newInstance();
                        }
                        catch (Exception e) {
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
                        }
                    }
                };
            }
            else {
                // ...or allocate new instance with unsafe, if it's not
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return GridUnsafe.allocateInstance(cls);
                        }
                        catch (InstantiationException e) {
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
                        }
                    }
                };
            }
        }
    }

    /**
     * Gets value of this element, if it's a {@link GridSqlConst}, or gets a param by index if element
     * is a {@link GridSqlParameter}
     *
     * @param element SQL element.
     * @param params Params to use if {@code element} is a {@link GridSqlParameter}.
     * @return column value.
     */
    private static Object getElementValue(GridSqlElement element, Object[] params) {
        if (element == null)
            return null;

        if (element instanceof GridSqlConst)
            return ((GridSqlConst)element).value().getObject();
        else if (element instanceof GridSqlParameter)
            return params[((GridSqlParameter)element).index()];
        else
            throw createSqlException("Unexpected SQL expression type [cls=" + element.getClass().getName() + ']',
                ErrorCode.UNKNOWN_DATA_TYPE_1);
    }

    /** */
    private final static class InsertEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to set. */
        private final Object val;

        /** */
        private InsertEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            if (entry.getValue() != null)
                return false;

            entry.setValue(val);
            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /**
     * Entry processor invoked by UPDATE and DELETE operations.
     */
    private final static class ModifyingEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to expect. */
        private final Object val;

        /** Action to perform on entry. */
        private final IgniteInClosure<MutableEntry<Object, Object>> entryModifier;

        /** */
        private ModifyingEntryProcessor(Object val, IgniteInClosure<MutableEntry<Object, Object>> entryModifier) {
            this.val = val;
            this.entryModifier = entryModifier;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            // Something happened to the cache while we were performing map-reduce.
            if (!F.eq(entry.getValue(), val))
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
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.setValue(val);
        }
    }

    /**
     * Method to construct new instances of keys and values on SQL MERGE and INSERT.
     */
    private interface Supplier extends GridPlainClosure<List<?>, Object> {
        // No-op.
    }

    /**
     * @param target Expression to extract the table from.
     * @return Back end table for this element.
     */
    private static GridSqlTable gridTableForElement(GridSqlElement target) {
        Set<GridSqlTable> tbls = new HashSet<>();

        DmlAstUtils.collectAllGridTablesInTarget(target, tbls);

        if (tbls.size() != 1)
            throw createSqlException("Failed to determine target table", ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        return tbls.iterator().next();
    }

    /**
     * Check that UPDATE statement affects no key columns.
     *
     * @param statement Statement.
     */
    private static void verifyUpdateColumns(GridSqlStatement statement) {
        if (statement == null || !(statement instanceof GridSqlUpdate))
            return;

        GridSqlUpdate update = (GridSqlUpdate) statement;

        GridSqlElement updTarget = update.target();

        Set<GridSqlTable> tbls = new HashSet<>();

        DmlAstUtils.collectAllGridTablesInTarget(updTarget, tbls);

        if (tbls.size() != 1)
            throw createSqlException("Failed to determine target table for UPDATE", ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        if (updateAffectsKeyColumns(gridTbl, update.set().keySet()))
            throw createSqlException("SQL UPDATE can't modify key or its fields directly", ErrorCode.COLUMN_NOT_FOUND_1);
    }

    /**
     * Check if given set of modified columns intersects with the set of SQL properties of the key.
     *
     * @param gridTbl Table.
     * @param affectedColNames Column names.
     * @return {@code true} if any of given columns corresponds to the key or any of its properties.
     */
    private static boolean updateAffectsKeyColumns(GridH2Table gridTbl, Set<String> affectedColNames) {
        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        Column[] cols = gridTbl.getColumns();

        // Check "_key" column itself - always has index of 0.
        if (affectedColNames.contains(cols[0].getName()))
            return true;

        // Start off from i = 2 to skip indices of 0 an 1 corresponding to key and value respectively.
        for (int i = 2; i < cols.length; i++)
            if (affectedColNames.contains(cols[i].getName()) && desc.isColumnKeyProperty(i - 2))
                return true;

        return false;
    }

    /**
     * Wrap result of DML operation (number of items affected) to Iterable suitable to be wrapped by cursor.
     *
     * @param itemsCnt Update result to wrap.
     * @return Resulting Iterable.
     */
    @SuppressWarnings("unchecked")
    private static QueryCursorImpl<List<?>> cursorForUpdateResult(long itemsCnt) {
        QueryCursorImpl<List<?>> res =
            new QueryCursorImpl(Collections.singletonList(Collections.singletonList(itemsCnt)), null, false);

        res.fieldsMeta(UPDATE_RESULT_META);

        return res;
    }

    /** Update result - modifications count and keys to re-run query with, if needed. */
    private final static class UpdateResult {
        /** Number of processed items. */
        final long cnt;

        /** Keys that failed to be UPDATEd or DELETEd due to concurrent modification of values. */
        @NotNull
        final Object[] errKeys;

        /** */
        @SuppressWarnings("ConstantConditions")
        private UpdateResult(long cnt, Object[] errKeys) {
            this.cnt = cnt;
            this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
        }
    }

    /** Result of processing an individual batch with {@link IgniteCache#invokeAll} including error details, if any. */
    private final static class BatchProcessingResult {
        /** Number of successfully processed items. */
        final long cnt;

        /** Keys that failed to be UPDATEd or DELETEd due to concurrent modification of values. */
        @NotNull
        final Object[] errKeys;

        /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
        final SQLException ex;

        /** */
        @SuppressWarnings("ConstantConditions")
        private BatchProcessingResult(long cnt, Object[] errKeys, SQLException ex) {
            this.cnt = cnt;
            this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
            this.ex = ex;
        }
    }

    /** Result of splitting keys whose processing resulted into an exception from those skipped by
     * logic of {@link EntryProcessor}s (most likely INSERT duplicates, or UPDATE/DELETE keys whose values
     * had been modified concurrently), counting and collecting entry processor exceptions.
     */
    private final static class BatchProcessingErrorResult {
        /** Keys that failed to be processed by {@link EntryProcessor} (not due to an exception). */
        @NotNull
        final Object[] errKeys;

        /** Number of entries whose processing resulted into an exception. */
        final int cnt;

        /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
        final SQLException ex;

        /** */
        @SuppressWarnings("ConstantConditions")
        private BatchProcessingErrorResult(@NotNull Object[] errKeys, SQLException ex, int exCnt) {
            errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
            // When exceptions count must be zero, exceptions chain must be not null, and vice versa.
            assert exCnt == 0 ^ ex != null;

            this.errKeys = errKeys;
            this.cnt = exCnt;
            this.ex = ex;
        }
    }

    /**
     * Update plan - initial statement, matching SELECT query, collocation flag, and single entry update filter.
     */
    private final static class UpdatePlan {
        /** Initial statement to drive the rest of the logic. */
        private final GridSqlStatement initStmt;

        /** Target element to be affected by {@link #initStmt}. */
        private final GridSqlElement target;

        /** SELECT statement built upon {@link #initStmt}. */
        private final GridSqlQuery selectQry;

        /** Subquery flag - returns {@code true} if {@link #selectQry} is a subquery, {@code false} otherwise. */
        private final boolean isSubqry;

        /** Entry filter for fast UPDATE or DELETE. */
        private final GridTriple<GridSqlElement> singleUpdate;

        /** */
        private UpdatePlan(GridSqlStatement initStmt, GridSqlElement target, GridSqlQuery selectQry,
                           boolean isSubqry, GridTriple<GridSqlElement> singleUpdate) {
            assert initStmt != null;
            assert target != null;

            this.initStmt = initStmt;
            this.target = target;
            this.selectQry = selectQry;
            this.isSubqry = isSubqry;
            this.singleUpdate = singleUpdate;
        }
    }
}
