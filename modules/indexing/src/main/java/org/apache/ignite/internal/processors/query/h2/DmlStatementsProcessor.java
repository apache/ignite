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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
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
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
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
     * @param prepared Prepared JDBC statement.
     * @param fieldsQry Original query.
     * @param loc Query locality flag.
     * @param filters Cache name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult updateSqlFields(String schemaName, Prepared prepared, SqlFieldsQuery fieldsQry,
        boolean loc, IndexingQueryFilter filters, GridQueryCancel cancel) throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        UpdatePlan plan = getPlanForStatement(schemaName, prepared, null);

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
                r = executeUpdateStatement(schemaName, cctx, prepared, fieldsQry, loc, filters, cancel, errKeys);
            }
            finally {
                cctx.operationContextPerCall(opCtx);
            }

            items += r.cnt;
            errKeys = r.errKeys;

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
     * @param p Prepared.
     * @param fieldsQry Initial query
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    QueryCursorImpl<List<?>> updateSqlFieldsDistributed(String schemaName, Prepared p,
        SqlFieldsQuery fieldsQry, GridQueryCancel cancel) throws IgniteCheckedException {
        UpdateResult res = updateSqlFields(schemaName, p, fieldsQry, false, null, cancel);

        QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
            (Collections.singletonList(res.cnt)), cancel, false);

        resCur.fieldsMeta(UPDATE_RESULT_META);

        return resCur;
    }

    /**
     * Execute DML statement on local cache.
     *
     * @param schemaName Schema.
     * @param stmt Prepared statement.
     * @param fieldsQry Fields query.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateSqlFieldsLocal(String schemaName, PreparedStatement stmt,
        SqlFieldsQuery fieldsQry, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        UpdateResult res = updateSqlFields(schemaName,  GridSqlQueryParser.prepared(stmt), fieldsQry, true,
            filters, cancel);

        return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META,
            new IgniteSingletonIterator(Collections.singletonList(res.cnt)));
    }

    /**
     * Perform given statement against given data streamer. Only rows based INSERT and MERGE are supported
     * as well as key bound UPDATE and DELETE (ones with filter {@code WHERE _key = ?}).
     *
     * @param streamer Streamer to feed data to.
     * @param stmt Statement.
     * @param args Statement arguments.
     * @return Number of rows in given statement for INSERT and MERGE, {@code 1} otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    long streamUpdateQuery(IgniteDataStreamer streamer, PreparedStatement stmt, Object[] args)
        throws IgniteCheckedException {
        args = U.firstNotNull(args, X.EMPTY_OBJECT_ARRAY);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        assert p != null;

        UpdatePlan plan = UpdatePlanBuilder.planForStatement(p, null);

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
    private UpdateResult executeUpdateStatement(String schemaName, final GridCacheContext cctx,
        Prepared prepared, SqlFieldsQuery fieldsQry, boolean loc, IndexingQueryFilter filters,
        GridQueryCancel cancel, Object[] failedKeys) throws IgniteCheckedException {
        int mainCacheId = CU.cacheId(cctx.name());

        Integer errKeysPos = null;

        UpdatePlan plan = getPlanForStatement(schemaName, prepared, errKeysPos);

        if (plan.fastUpdateArgs != null) {
            assert F.isEmpty(failedKeys) && errKeysPos == null;

            return doFastUpdate(plan, fieldsQry.getArgs());
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
     *
     * @param schema Schema.
     * @param p Prepared JDBC statement.
     * @return Update plan.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private UpdatePlan getPlanForStatement(String schema, Prepared p, @Nullable Integer errKeysPos)
        throws IgniteCheckedException {
        H2DmlPlanKey planKey = new H2DmlPlanKey(schema, p.getSQL());

        UpdatePlan res = (errKeysPos == null ? planCache.get(planKey) : null);

        if (res != null)
            return res;

        res = UpdatePlanBuilder.planForStatement(p, errKeysPos);

        // Don't cache re-runs
        if (errKeysPos == null)
            return U.firstNotNull(planCache.putIfAbsent(planKey, res), res);
        else
            return res;
    }

    /**
     * Perform single cache operation based on given args.
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

                assert prop != null;

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
                    IgniteQueryErrorCode.DUPLICATE_KEY);
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

        if (key == null)
            throw new IgniteSQLException("Key for INSERT or MERGE must not be null",  IgniteQueryErrorCode.NULL_KEY);

        if (val == null)
            throw new IgniteSQLException("Value for INSERT or MERGE must not be null", IgniteQueryErrorCode.NULL_VALUE);

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

    /** */
    private final static class InsertEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
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

    /** Update result - modifications count and keys to re-run query with, if needed. */
    private final static class UpdateResult {
        /** Result to return for operations that affected 1 item - mostly to be used for fast updates and deletes. */
        final static UpdateResult ONE = new UpdateResult(1, X.EMPTY_OBJECT_ARRAY);

        /** Result to return for operations that affected 0 items - mostly to be used for fast updates and deletes. */
        final static UpdateResult ZERO = new UpdateResult(0, X.EMPTY_OBJECT_ARRAY);

        /** Number of processed items. */
        final long cnt;

        /** Keys that failed to be updated or deleted due to concurrent modification of values. */
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
