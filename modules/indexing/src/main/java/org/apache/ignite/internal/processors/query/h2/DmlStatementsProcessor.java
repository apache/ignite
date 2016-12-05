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
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
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
import org.apache.ignite.internal.processors.query.h2.dml.FastUpdateArguments;
import org.apache.ignite.internal.processors.query.h2.dml.KeyValueSupplier;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
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
    @SuppressWarnings("ConstantConditions")
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

        assert !F.isEmpty(plan.selectQry);

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // subquery and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocSubqry) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQry, fieldsQry.isCollocated())
                .setArgs(params)
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS);

            cur = (QueryCursorImpl<List<?>>) indexing.queryTwoStep(cctx, newFieldsQry, cancel);
        }
        else {
            final GridQueryFieldsResult res = indexing.queryLocalSqlFields(cctx.name(), plan.selectQry, F.asList(params),
                filters, fieldsQry.isEnforceJoinOrder(), fieldsQry.getTimeout(), cancel);

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), cctx, cctx.keepBinary());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);

            cur.fieldsMeta(res.metaData());
        }

        int pageSize = loc ? 0 : fieldsQry.getPageSize();

        switch (plan.mode) {
            case MERGE:
                return new UpdateResult(doMerge(plan, cur, pageSize), X.EMPTY_OBJECT_ARRAY);

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
    private UpdateResult doDelete(GridCacheContext cctx, QueryCursorImpl<List<?>> cursor, int pageSize)
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
    private UpdateResult doUpdate(UpdatePlan plan, QueryCursorImpl<List<?>> cursor, int pageSize)
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

        long res = 0;

        Map<Object, EntryProcessor<Object, Object, Boolean>> rows = new LinkedHashMap<>();

        // Keys that failed to UPDATE due to concurrent updates.
        List<Object> failedKeys = new ArrayList<>();

        SQLException resEx = null;

        Iterator<List<?>> it = cursor.iterator();

        while (it.hasNext()) {
            List<?> e = it.next();
            Object key = e.get(0);
            Object val = (hasNewVal ? e.get(valColIdx) : e.get(1));

            Object newVal;

            Map<String, Object> newColVals = new HashMap<>();

            for (int i = 0; i < plan.colNames.length; i++) {
                if (hasNewVal && i == valColIdx - 2)
                    continue;

                newColVals.put(plan.colNames[i], convert(e.get(i + 2), plan.colNames[i],
                    plan.tbl.rowDescriptor(), plan.colTypes[i]));
            }

            newVal = plan.valSupplier.apply(e);

            if (newVal == null)
                throw new IgniteSQLException("New value for UPDATE must not be null", IgniteQueryErrorCode.NULL_VALUE);

            if (bin && !(val instanceof BinaryObject))
                val = cctx.grid().binary().toBinary(val);

            // Skip key and value - that's why we start off with 2nd column
            for (int i = 0; i < plan.tbl.getColumns().length - 2; i++) {
                Column c = plan.tbl.getColumn(i + 2);

                GridQueryProperty prop = desc.type().property(c.getName());

                if (prop.key())
                    continue; // Don't get values of key's columns - we won't use them anyway

                boolean hasNewColVal = newColVals.containsKey(c.getName());

                // Binary objects get old field values from the Builder, so we can skip what we're not updating
                if (bin && !hasNewColVal)
                    continue;

                // Column values that have been explicitly specified have priority over field values in old or new _val
                // If no value given for the column, then we expect to find it in value, and not in key - hence null arg.
                Object colVal = hasNewColVal ? newColVals.get(c.getName()) : prop.value(null, val);

                // UPDATE currently does not allow to modify key or its fields, so we must be safe to pass null as key.
                desc.setColumnValue(null, newVal, colVal, i);
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
     * @param type Expected column type to convert to.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    private static Object convert(Object val, String colName, GridH2RowDescriptor desc, int type)
        throws IgniteCheckedException {
        if (val == null)
            return null;

        GridQueryProperty prop = desc.type().property(colName);

        assert prop != null;

        Class<?> expCls = prop.type();

        Class<?> currCls = val.getClass();

        if (val instanceof Date && currCls != Date.class && expCls == Date.class) {
            // H2 thinks that java.util.Date is always a Timestamp, while binary marshaller expects
            // precise Date instance. Let's satisfy it.
            return new Date(((Date) val).getTime());
        }

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

        return h2Val.convertTo(type).getObject();
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
    private long doMerge(UpdatePlan plan, QueryCursorImpl<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        // If we have just one item to put, just do so
        if (plan.rowsNum == 1) {
            IgniteBiTuple t = rowToKeyValue(cctx, cursor.iterator().next().toArray(), plan.colNames, plan.colTypes, plan.keySupplier,
                plan.valSupplier, plan.keyColIdx, plan.valColIdx, desc);

            cctx.cache().put(t.getKey(), t.getValue());
            return 1;
        }
        else {
            int resCnt = 0;
            Map<Object, Object> rows = new LinkedHashMap<>();

            for (Iterator<List<?>> it = cursor.iterator(); it.hasNext();) {
                List<?> row = it.next();

                IgniteBiTuple t = rowToKeyValue(cctx, row.toArray(), plan.colNames, plan.colTypes, plan.keySupplier, plan.valSupplier,
                    plan.keyColIdx, plan.valColIdx, desc);

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
    private long doInsert(UpdatePlan plan, QueryCursorImpl<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridH2RowDescriptor desc = plan.tbl.rowDescriptor();

        GridCacheContext cctx = desc.context();

        // If we have just one item to put, just do so
        if (plan.rowsNum == 1) {
            IgniteBiTuple t = rowToKeyValue(cctx, cursor.iterator().next().toArray(), plan.colNames, plan.colTypes,
                plan.keySupplier, plan.valSupplier, plan.keyColIdx, plan.valColIdx, desc);

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw new IgniteSQLException("Duplicate key during INSERT [key=" + t.getKey() + ']',
                    IgniteQueryErrorCode.DUPLICATE_KEY);
        }
        else {
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

                final IgniteBiTuple t = rowToKeyValue(cctx, row.toArray(), plan.colNames, plan.colTypes, plan.keySupplier,
                    plan.valSupplier, plan.keyColIdx, plan.valColIdx, desc);

                rows.put(t.getKey(), new InsertEntryProcessor(t.getValue()));

                if (!it.hasNext() || (pageSize > 0 && rows.size() == pageSize)) {
                    PageProcessingResult pageRes = processPage(cctx, rows);

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
     * @param cols Query cols.
     * @param colTypes Column types to convert data from {@code row} to.
     * @param keySupplier Key instantiation method.
     * @param valSupplier Key instantiation method.
     * @param keyColIdx Key column index, or {@code -1} if no key column is mentioned in {@code cols}.
     * @param valColIdx Value column index, or {@code -1} if no value column is mentioned in {@code cols}.
     * @param rowDesc Row descriptor.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ResultOfMethodCallIgnored"})
    private IgniteBiTuple<?, ?> rowToKeyValue(GridCacheContext cctx, Object[] row, String[] cols,
        int[] colTypes, KeyValueSupplier keySupplier, KeyValueSupplier valSupplier, int keyColIdx, int valColIdx,
        GridH2RowDescriptor rowDesc) throws IgniteCheckedException {
        Object key = keySupplier.apply(F.asList(row));
        Object val = valSupplier.apply(F.asList(row));

        if (key == null)
            throw new IgniteSQLException("Key for INSERT or MERGE must not be null",  IgniteQueryErrorCode.NULL_KEY);

        if (val == null)
            throw new IgniteSQLException("Value for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_VALUE);

        GridQueryTypeDescriptor desc = rowDesc.type();

        for (int i = 0; i < cols.length; i++) {
            if (i == keyColIdx || i == valColIdx)
                continue;

            desc.setValue(cols[i], key, val, convert(row[i], cols[i], rowDesc, colTypes[i]));
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
            if (entry.exists())
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
            assert val != null;

            this.val = val;
            this.entryModifier = entryModifier;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
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
