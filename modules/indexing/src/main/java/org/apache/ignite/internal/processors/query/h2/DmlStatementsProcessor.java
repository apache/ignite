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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.dml.*;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.table.Column;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 *
 */
public class DmlStatementsProcessor {
    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private final static int DFLT_DML_RERUN_ATTEMPTS = 4;

    /** Indexing. */
    private final IgniteH2Indexing indexing;

    /** Logger. */
    private static IgniteLogger log;

    /** Set of binary type ids for which warning about missing identity in configuration has been printed. */
    private final static Set<Integer> WARNED_TYPES =
        Collections.newSetFromMap(new ConcurrentHashMap8<Integer, Boolean>());

    /** Default size for update plan cache. */
    private static final int PLAN_CACHE_SIZE = 1024;

    /** Update plans cache. */
    private final ConcurrentMap<String, ConcurrentMap<String, UpdatePlan>> planCache = new ConcurrentHashMap<>();

    /** Dummy metadata for update result. */
    private final static List<GridQueryFieldMetadata> UPDATE_RESULT_META = Collections.<GridQueryFieldMetadata>
        singletonList(new IgniteH2Indexing.SqlFieldMetadata(null, null, "UPDATED", Long.class.getName()));

    /**
     * @param indexing indexing.
     */
    DmlStatementsProcessor(IgniteH2Indexing indexing) {
        this.indexing = indexing;

        assert log == null;

        log = indexing.getLogger();
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param spaceName Space name.
     * @param stmt JDBC statement.
     * @param fieldsQry Original query.
     * @param loc Query locality flag.
     * @param filters Space name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private long updateSqlFields(String spaceName, PreparedStatement stmt, SqlFieldsQuery fieldsQry,
        boolean loc, IndexingQueryFilter filters, GridQueryCancel cancel) throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        UpdatePlan plan = getPlanForStatement(spaceName, stmt, null);

        GridCacheContext<?, ?> cctx = plan.tbl.rowDescriptor().context();

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
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

            UpdateResult r;

            try {
                r = executeUpdateStatement(cctx, stmt, fieldsQry, loc, filters,
                    cancel, errKeys);
            }
            finally {
                cctx.operationContextPerCall(opCtx);
            }

            if (F.isEmpty(r.errKeys))
                return r.cnt + items;
            else {
                items += r.cnt;
                errKeys = r.errKeys;
            }
        }

        throw new IgniteSQLException("Failed to update or delete some keys: " + Arrays.deepToString(errKeys),
            IgniteQueryErrorCode.CONCURRENT_UPDATE);
    }

    /**
     * @param spaceName Space name.
     * @param stmt Prepared statement.
     * @param fieldsQry Initial query.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    QueryCursorImpl<List<?>> updateSqlFieldsTwoStep(String spaceName, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, GridQueryCancel cancel) throws IgniteCheckedException {
        long res = updateSqlFields(spaceName, stmt, fieldsQry, false, null, cancel);

        return cursorForUpdateResult(res);
    }

    /**
     * Execute DML statement on local cache.
     * @param spaceName Space name.
     * @param stmt Prepared statement.
     * @param filters Space name and key filter.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateLocalSqlFields(String spaceName, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, IndexingQueryFilter filters, GridQueryCancel cancel) throws IgniteCheckedException {
        long res = updateSqlFields(spaceName, stmt, fieldsQry, true, filters, cancel);

        return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META,
            new IgniteSingletonIterator(Collections.singletonList(res)));
    }

    /**
     * Actually perform SQL DML operation locally.
     * @param cctx Cache context.
     * @param prepStmt Prepared statement for DML query.
     * @param filters Space name and key filter.
     * @param failedKeys Keys to restrict UPDATE and DELETE operations with. Null or empty array means no restriction.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private UpdateResult executeUpdateStatement(final GridCacheContext cctx, PreparedStatement prepStmt,
        SqlFieldsQuery fieldsQry, boolean loc, IndexingQueryFilter filters, GridQueryCancel cancel, Object[] failedKeys)
        throws IgniteCheckedException {
        Integer errKeysPos = null;

        Object[] params = fieldsQry.getArgs();

        if (!F.isEmpty(failedKeys)) {
            int paramsCnt = F.isEmpty(params) ? 0 : params.length;
            params = Arrays.copyOf(U.firstNotNull(params, X.EMPTY_OBJECT_ARRAY), paramsCnt + 1);
            params[paramsCnt] = failedKeys;
            errKeysPos = paramsCnt; // Last position
        }

        UpdatePlan plan = getPlanForStatement(cctx.name(), prepStmt, errKeysPos);

        if (plan.fastUpdateArgs != null) {
            assert F.isEmpty(failedKeys) && errKeysPos == null;

            return new UpdateResult(doSingleUpdate(plan, params), X.EMPTY_OBJECT_ARRAY);
        }

        assert !F.isEmpty(plan.rows) ^ !F.isEmpty(plan.selectQry);

        Iterable<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // subquery and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocSubqry) {
            assert !F.isEmpty(plan.selectQry);

            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQry, fieldsQry.isCollocated())
                .setArgs(params)
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = indexing.queryTwoStep(cctx, newFieldsQry, cancel);
        }
        else if (F.isEmpty(plan.rows)) {
            final GridQueryFieldsResult res = indexing.queryLocalSqlFields(cctx.name(), plan.selectQry, F.asList(params),
                filters, fieldsQry.isEnforceJoinOrder(), fieldsQry.getTimeout(), cancel);

            QueryCursorImpl<List<?>> resCur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), cctx, cctx.keepBinary());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);

            resCur.fieldsMeta(res.metaData());

            cur = resCur;
        }
        else {
            assert plan.rowsNum > 0 && !F.isEmpty(plan.colNames);

            List<List<?>> args = new ArrayList<>(plan.rowsNum);

            GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

            for (List<FastUpdateArgument> argRow : plan.rows) {
                List<Object> row = new ArrayList<>();

                for (int j = 0; j < plan.colNames.length; j++) {
                    Object colVal = argRow.get(j).apply(fieldsQry.getArgs());

                    if (j == plan.keyColIdx || j == plan.valColIdx) // The rest columns will be converted later
                        colVal = convert(colVal, j == plan.keyColIdx ? desc.type().keyClass() : desc.type().valueClass(),
                            desc);

                    row.add(colVal);
                }

                args.add(row);
            }

            cur = args;
        }

        int pageSize = loc ? 0 : fieldsQry.getPageSize();

        switch (plan.mode) {
            case MERGE:
                if (plan.valColIdx != -1) // _val column is given explicitly - we can just call 'put' directly
                    return new UpdateResult(doSimpleMerge(plan, cur, pageSize), X.EMPTY_OBJECT_ARRAY);
                else // We have to use entry processors
                    return new UpdateResult(doComplexMerge(plan, cur, pageSize), X.EMPTY_OBJECT_ARRAY);

            case INSERT:
                return new UpdateResult(doInsert(plan, cur, pageSize), X.EMPTY_OBJECT_ARRAY);

            case UPDATE:
                return doUpdate(plan, cur, pageSize);

            case DELETE:
                return doDelete(cctx, cur, pageSize);

            default:
                throw new IgniteSQLException("Unexpected DML operation [mode=" + plan.mode + ']',
                    IgniteQueryErrorCode.UNEXPECTED_OPERATION);
        }
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     * @param spaceName Space name.
     * @param prepStmt JDBC statement.
     * @return Update plan.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private UpdatePlan getPlanForStatement(String spaceName, PreparedStatement prepStmt,
        @Nullable Integer errKeysPos) throws IgniteCheckedException {
        Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement) prepStmt);

        spaceName = F.isEmpty(spaceName) ? "default" : spaceName;

        ConcurrentMap<String, UpdatePlan> spacePlans = planCache.get(spaceName);

        if (spacePlans == null) {
            spacePlans = new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

            spacePlans = U.firstNotNull(planCache.putIfAbsent(spaceName, spacePlans), spacePlans);
        }

        // getSQL returns field value, so it's fast
        // Don't look for re-runs in cache, we don't cache them
        UpdatePlan res = (errKeysPos == null ? spacePlans.get(p.getSQL()) : null);

        if (res != null)
            return res;

        res = UpdatePlanBuilder.planForStatement(p, errKeysPos);

        // Don't cache re-runs
        if (errKeysPos == null)
            return U.firstNotNull(spacePlans.putIfAbsent(p.getSQL(), res), res);
        else
            return res;
    }

    /**
     * Perform single cache operation based on given args.
     * @param params Query parameters.
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static long doSingleUpdate(UpdatePlan plan, Object[] params) throws IgniteCheckedException {
        GridCacheContext cctx = plan.tbl.rowDescriptor().context();

        FastUpdateArguments singleUpdate = plan.fastUpdateArgs;

        assert singleUpdate != null;

        int res;

        Object key = singleUpdate.key.apply(params);
        Object val = singleUpdate.val.apply(params);
        Object newVal = singleUpdate.newVal.apply(params);

        if (newVal != null) { // Single item UPDATE
            if (val == null) // No _val bound in source query
                res = cctx.cache().replace(key, newVal) ? 1 : 0;
            else
                res = cctx.cache().replace(key, val, newVal) ? 1 : 0;
        }
        else { // Single item DELETE
            if (val == null) // No _val bound in source query
                res = cctx.cache().remove(key) ? 1 : 0;
            else
                res = cctx.cache().remove(key, val) ? 1 : 0;
        }

        return res;
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
        // With DELETE, we have only two columns - key and value.
        long res = 0;

        // Keys that failed to DELETE due to concurrent updates.
        List<Object> failedKeys = new ArrayList<>();

        SQLException resEx = null;


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
                PageProcessingResult pageRes = processPage(cctx, rows);

                res += pageRes.cnt;

                failedKeys.addAll(F.asList(pageRes.errKeys));

                if (pageRes.ex != null) {
                    if (resEx == null)
                        resEx = pageRes.ex;
                    else
                        resEx.setNextException(pageRes.ex);
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

                SQLException conEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

                conEx.setNextException(resEx);

                resEx = conEx;
            }

            throw new IgniteSQLException(resEx);
        }

        return new UpdateResult(res, failedKeys.toArray());
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

        String[] propNames = plan.colNames;

        long res = 0;

        Map<Object, EntryProcessor<Object, Object, Boolean>> rows = new LinkedHashMap<>();

        // Keys that failed to UPDATE due to concurrent updates.
        List<Object> failedKeys = new ArrayList<>();

        SQLException resEx = null;

        Iterator<List<?>> it = cursor.iterator();

        DmlEntryProcessorArgs args = new DmlEntryProcessorArgs(desc.type().name(), plan.props);

        int valColIdx = plan.valColIdx;

        // Actual position of new _val in args' array
        if (valColIdx != -1)
            valColIdx -= 2;

        while (it.hasNext()) {
            List<?> e = it.next();

            Object key = e.get(0);

            Object srcVal = e.get(1);

            Object[] newColVals = e.subList(2, propNames.length + 2).toArray();

            for (int i = 0; i < propNames.length; i++)
                if (i != valColIdx)
                    newColVals[i] = convert(newColVals[i], propNames[i], desc);

            if (bin && !(srcVal instanceof BinaryObject))
                srcVal = cctx.grid().binary().toBinary(srcVal);

            Object newVal = null;

            if (plan.valSupplier != null)
                newVal = plan.valSupplier.apply(e);

            rows.put(key, new ModifyingEntryProcessor(srcVal, new UpdateProcessor(newColVals, newVal)));

            if ((pageSize > 0 && rows.size() == pageSize) || (!it.hasNext())) {
                PageProcessingResult pageRes = processPage(cctx, rows, args);

                res += pageRes.cnt;

                failedKeys.addAll(F.asList(pageRes.errKeys));

                if (pageRes.ex != null) {
                    if (resEx == null)
                        resEx = pageRes.ex;
                    else
                        resEx.setNextException(pageRes.ex);
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

                SQLException dupEx = createJdbcSqlException(msg, IgniteQueryErrorCode.CONCURRENT_UPDATE);

                dupEx.setNextException(resEx);

                resEx = dupEx;
            }

            throw new IgniteSQLException(resEx);
        }

        return new UpdateResult(res, failedKeys.toArray());
    }

    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param colName Column name to search for property.
     * @param desc Row descriptor.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    private static Object convert(Object val, String colName, GridH2RowDescriptor desc)
        throws IgniteCheckedException {
        if (val == null)
            return null;

        GridQueryProperty prop = desc.type().property(colName);

        assert prop != null;

        Class<?> expCls = prop.type();

        return convert(val, expCls, desc);
    }

    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param expCls Expected property class.
     * @param desc Row descriptor.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    private static Object convert(Object val, Class<?> expCls, GridH2RowDescriptor desc)
        throws IgniteCheckedException {
        Class<?> currCls = val.getClass();

        if (val instanceof Date && currCls != Date.class && expCls == Date.class) {
            // H2 thinks that java.util.Date is always a Timestamp, while binary marshaller expects
            // precise Date instance. Let's satisfy it.
            return new Date(((Date) val).getTime());
        }

        int type = DataType.getTypeFromClass(expCls);

        // We have to convert arrays of reference types manually - see https://issues.apache.org/jira/browse/IGNITE-4327
        // Still, we only can convert from Object[] to something more precise.
        if (type == Value.ARRAY && currCls != expCls) {
            if (currCls != Object[].class)
                throw new IgniteCheckedException("Unexpected array type - only conversion from Object[] is assumed");

            // Why would otherwise type be Value.ARRAY?
            assert expCls.isArray();

            Object[] curr = (Object[]) val;

            Object newArr = Array.newInstance(expCls.getComponentType(), curr.length);

            System.arraycopy(curr, 0, newArr, 0, curr.length);

            return newArr;
        }

        int objType = DataType.getTypeFromClass(val.getClass());

        if (objType == type)
            return val;

        Value h2Val = desc.wrap(val, objType);

        Object res = h2Val.convertTo(type).getObject();

        if (res instanceof Date && res.getClass() != Date.class && expCls == Date.class) {
            // We can get a Timestamp instead of Date when converting a String to Date without query - let's handle this
            return new Date(((Date) res).getTime());
        }

        return res;
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
    private long doSimpleMerge(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize) throws IgniteCheckedException {
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
     * Perform entry processors based MERGE.
     * @param plan Update plan.
     * @param cur Cursor with rows to process.
     * @param pageSize Page size.
     * @return Number of affected items.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private long doComplexMerge(UpdatePlan plan, Iterable<List<?>> cur, int pageSize) throws IgniteCheckedException {
        assert plan.valColIdx == -1;

        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        DmlEntryProcessorArgs args = new DmlEntryProcessorArgs(desc.type().name(), plan.props);

        // If we have just one item to process, just do so
        if (plan.rowsNum == 1) {
            IgniteBiTuple<Object, EntryProcessor> t = rowToInsertProc(cctx, cur.iterator().next(), plan);

            cctx.cache().invoke(t.get1(), t.get2(), args);
            return 1;
        }
        else {
            int resCnt = 0;
            Map<Object, Object> rows = new LinkedHashMap<>();

            for (Iterator<List<?>> it = cur.iterator(); it.hasNext();) {
                List<?> row = it.next();

                IgniteBiTuple<Object, EntryProcessor> t = rowToInsertProc(cctx, row, plan);

                rows.put(t.getKey(), t.getValue());

                if ((pageSize > 0 && rows.size() == pageSize) || !it.hasNext()) {
                    cctx.cache().invokeAll(rows, args);
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
                    IgniteQueryErrorCode.DUPLICATE_KEY);
        }
        else {
            DmlEntryProcessorArgs args = new DmlEntryProcessorArgs(desc.type().name(), plan.props);

            Map<Object, EntryProcessor<Object, Object, Boolean>> rows = plan.isLocSubqry ?
                new LinkedHashMap<Object, EntryProcessor<Object, Object, Boolean>>(plan.rowsNum) :
                new LinkedHashMap<Object, EntryProcessor<Object, Object, Boolean>>();

            // Keys that failed to INSERT due to duplication.
            List<Object> duplicateKeys = new ArrayList<>();

            int resCnt = 0;

            SQLException resEx = null;

            Iterator<List<?>> it = cursor.iterator();

            while (it.hasNext()) {
                List<?> row = it.next();

                final IgniteBiTuple<Object, EntryProcessor> t = rowToInsertProc(cctx, row, plan);

                rows.put(t.getKey(), t.getValue());

                if (!it.hasNext() || (pageSize > 0 && rows.size() == pageSize)) {
                    PageProcessingResult pageRes = processPage(cctx, rows, args);

                    resCnt += pageRes.cnt;

                    duplicateKeys.addAll(F.asList(pageRes.errKeys));

                    if (pageRes.ex != null) {
                        if (resEx == null)
                            resEx = pageRes.ex;
                        else
                            resEx.setNextException(pageRes.ex);
                    }

                    rows.clear();
                }
            }

            if (!F.isEmpty(duplicateKeys)) {
                String msg = "Failed to INSERT some keys because they are already in cache " +
                    "[keys=" + duplicateKeys + ']';

                SQLException dupEx = new SQLException(msg, null, IgniteQueryErrorCode.DUPLICATE_KEY);

                if (resEx == null)
                    resEx = dupEx;
                else
                    resEx.setNextException(dupEx);
            }

            if (resEx != null)
                throw new IgniteSQLException(resEx);

            return resCnt;
        }
    }

    /**
     * Execute given entry processors and collect errors, if any.
     * @param cctx Cache context.
     * @param rows Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     * @throws IgniteCheckedException
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static PageProcessingResult processPage(GridCacheContext cctx,
        Map<Object, EntryProcessor<Object, Object, Boolean>> rows, Object... args) throws IgniteCheckedException {
        Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows, args);

        if (F.isEmpty(res))
            return new PageProcessingResult(rows.size(), null, null);

        PageProcessingErrorResult splitRes = splitErrors(res);

        int keysCnt = splitRes.errKeys.length;

        return new PageProcessingResult(rows.size() - keysCnt - splitRes.cnt, splitRes.errKeys, splitRes.ex);
    }

    /**
     * Convert row presented as an array of Objects into key-value pair to be inserted to cache.
     * To be used in simple MERGE cases - i.e. when _val column value is given, hence we can perform
     * cache puts directly, without entry processors.
     * @param cctx Cache context.
     * @param row Row to process.
     * @param plan Update plan.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ResultOfMethodCallIgnored"})
    private IgniteBiTuple<?, ?> rowToKeyValue(GridCacheContext cctx, List<?> row, UpdatePlan plan)
        throws IgniteCheckedException {
        Object key = plan.keySupplier.apply(row);
        Object val = plan.valSupplier.apply(row);

        if (key == null)
            throw new IgniteSQLException("Key for INSERT or MERGE must not be null",  IgniteQueryErrorCode.NULL_KEY);

        if (val == null)
            throw new IgniteSQLException("Value for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_VALUE);

        GridQueryTypeDescriptor desc = plan.tbl.rowDescriptor().type();

        Map<String, Object> newColVals = new HashMap<>();

        for (int i = 0; i < plan.colNames.length; i++) {
            if (i == plan.keyColIdx || i == plan.valColIdx)
                continue;

            newColVals.put(plan.colNames[i], convert(row.get(i), plan.colNames[i], plan.tbl.rowDescriptor()));
        }

        // We update columns in the order specified by the table for a reason - table's
        // column order preserves their precedence for correct update of nested properties.
        Column[] cols = plan.tbl.getColumns();

        // First 2 columns are _key and _val, skip 'em.
        for (int i = 2; i < cols.length; i++) {
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

            if (key instanceof BinaryObject)
                key = updateHashCodeIfNeeded(cctx, (BinaryObject) key);

            if (val instanceof BinaryObject)
                val = updateHashCodeIfNeeded(cctx, (BinaryObject) val);
        }

        return new IgniteBiTuple<>(key, val);
    }

    /**
     * Create key and entry processor to perform atomic MERGE or INSERT (based on {@link UpdatePlan#mode}).
     * @param cctx Cache context.
     * @param row Column values to set.
     * @param plan Update plan.
     * @return Tuple [key; {@link MergeProcessor} for given column values]
     * @throws IgniteCheckedException if failed.
     */
    private IgniteBiTuple<Object, EntryProcessor> rowToInsertProc(GridCacheContext cctx, List<?> row,
        UpdatePlan plan) throws IgniteCheckedException {
        assert plan.mode == UpdateMode.INSERT || (plan.mode == UpdateMode.MERGE && plan.valColIdx == -1);

        Object key = plan.keySupplier.apply(row);

        if (key == null)
            throw new IgniteSQLException("Key for INSERT or MERGE must not be null",  IgniteQueryErrorCode.NULL_KEY);

        Object val = (plan.valSupplier != null ? plan.valSupplier.apply(row) : null);

        GridQueryTypeDescriptor desc = plan.tbl.rowDescriptor().type();

        // This array will have nulls at positions corresponding to key fields.
        Object[] newColVals = new Object[plan.colNames.length];

        int i = 0;

        for (String propName : desc.fields().keySet()) {
            Integer idx = plan.props.get(i++);

            if (idx == null)
                continue;

            GridQueryProperty prop = desc.property(propName);

            Object v = convert(row.get(idx), prop.type(), plan.tbl.rowDescriptor());

            if (prop.key())
                prop.setValue(key, null, v);
            else
                newColVals[idx] = v;
        }

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder) key).build();

            if (key instanceof BinaryObject)
                key = updateHashCodeIfNeeded(cctx, (BinaryObject) key);
        }

        return new IgniteBiTuple<>(key, (plan.mode == UpdateMode.MERGE ? new MergeProcessor(newColVals, val) :
            new InsertProcessor(newColVals, val)));
    }

    /**
     * Set hash code to binary object if it does not have one.
     *
     * @param cctx Cache context.
     * @param binObj Binary object.
     * @return Binary object with hash code set.
     */
    public static BinaryObject updateHashCodeIfNeeded(GridCacheContext cctx, BinaryObject binObj) {
        if (U.isHashCodeEmpty(binObj)) {
            if (WARNED_TYPES.add(binObj.type().typeId()))
                U.warn(log, "Binary object's type does not have identity resolver explicitly set, therefore " +
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

    /** */
    private static DmlEntryProcessor RMV = new DmlEntryProcessor() {
        /** {@inheritDoc} */
        @Override public void applyx(CacheInvokeEntry<Object, Object> e, DmlEntryProcessorArgs args)
            throws IgniteCheckedException {
            assert e.exists();

            e.remove();
        }
    };

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

    /** Result of processing an individual page with {@link IgniteCache#invokeAll} including error details, if any. */
    private final static class PageProcessingResult {
        /** Number of successfully processed items. */
        final long cnt;

        /** Keys that failed to be UPDATEd or DELETEd due to concurrent modification of values. */
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
}
