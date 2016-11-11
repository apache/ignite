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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
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
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatementSplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridPlainClosure;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.table.Column;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DFLT_DML_RERUN_ATTEMPTS;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.createSqlException;

/**
 *
 */
class DmlStatementsProcessor {
    /**
     * Indexing.
     */
    private final IgniteH2Indexing indexing;

    /** Set of binary type ids for which warning about missing identity in configuration has been printed. */
    private final static Set<Integer> WARNED_TYPES =
        Collections.newSetFromMap(new ConcurrentHashMap8<Integer, Boolean>());

    /**
     * @param indexing indexing.
     */
    DmlStatementsProcessor(IgniteH2Indexing indexing) {
        this.indexing = indexing;
    }

    /**
     * @param cctx Cache context.
     * @param stmt Prepared statement.
     * @param params Query parameters.
     * @param filters Space name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param timeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult updateLocalSqlFields(final GridCacheContext cctx, final PreparedStatement stmt,
        final Object[] params, final IndexingQueryFilter filters, boolean enforceJoinOrder, int timeout,
        GridQueryCancel cancel) throws IgniteCheckedException {
        Object[] errKeys = null;

        int items = 0;

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            IgniteBiTuple<Integer, Object[]> r = updateLocalSqlFields0(cctx, stmt, params, errKeys, filters,
                enforceJoinOrder, timeout, cancel);

            if (F.isEmpty(r.get2())) {
                return new GridQueryFieldsResultAdapter(Collections.<GridQueryFieldMetadata>emptyList(),
                    new IgniteSingletonIterator(Collections.singletonList(items + r.get1())));
            }
            else {
                items += r.get1();
                errKeys = r.get2();
            }
        }

        throw createSqlException("Failed to update or delete some keys: " + Arrays.deepToString(errKeys),
            ErrorCode.CONCURRENT_UPDATE_1);
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param cctx Cache context.
     * @param prepStmt Prepared statement for DML query.
     * @param params Query params.
     * @param failedKeys Keys to restrict UPDATE and DELETE operations with. Null or empty array means no restriction.
     * @param filters Filters.
     * @param enforceJoinOrder Enforce join order.
     * @param timeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private IgniteBiTuple<Integer, Object[]> updateLocalSqlFields0(final GridCacheContext cctx,
        final PreparedStatement prepStmt, Object[] params, final Object[] failedKeys, final IndexingQueryFilter filters,
        boolean enforceJoinOrder, int timeout, GridQueryCancel cancel) throws IgniteCheckedException {
        Connection conn = indexing.connectionForSpace(cctx.name());

        indexing.initLocalQueryContext(conn, enforceJoinOrder, filters);

        try {
            Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement) prepStmt);

            GridSqlStatement stmt = new GridSqlQueryParser().parse(p);

            if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert) {
                GridSqlQuery sel;

                if (stmt instanceof GridSqlInsert) {
                    GridSqlInsert ins = (GridSqlInsert) stmt;
                    sel = GridSqlStatementSplitter.selectForInsertOrMerge(ins.rows(), ins.query());
                }
                else {
                    GridSqlMerge merge = (GridSqlMerge) stmt;
                    sel = GridSqlStatementSplitter.selectForInsertOrMerge(merge.rows(), merge.query());
                }

                ResultSet rs = indexing.executeSqlQueryWithTimer(cctx.name(), conn, sel.getSQL(), F.asList(params), true,
                    timeout, cancel);

                final Iterator<List<?>> rsIter = new IgniteH2Indexing.FieldsIterator(rs);

                Iterable<List<?>> it = new Iterable<List<?>>() {
                    /** {@inheritDoc} */
                    @Override public Iterator<List<?>> iterator() {
                        return rsIter;
                    }
                };

                QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(it);

                int res;

                if (stmt instanceof GridSqlMerge)
                    res = doMerge(cctx, (GridSqlMerge) stmt, cur, 0);
                else
                    res = doInsert(cctx, (GridSqlInsert) stmt, cur, 0);

                return new IgniteBiTuple<>(res, X.EMPTY_OBJECT_ARRAY);
            }
            else {
                if (F.isEmpty(failedKeys)) {
                    GridTriple<GridSqlElement> singleUpdate;

                    if (stmt instanceof GridSqlUpdate)
                        singleUpdate = GridSqlStatementSplitter.getSingleItemFilter((GridSqlUpdate) stmt);
                    else if (stmt instanceof GridSqlDelete)
                        singleUpdate = GridSqlStatementSplitter.getSingleItemFilter((GridSqlDelete) stmt);
                    else
                        throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                            ErrorCode.PARSE_ERROR_1);

                    if (singleUpdate != null)
                        return new IgniteBiTuple<>(doSingleUpdate(cctx, singleUpdate, stmt, params), X.EMPTY_OBJECT_ARRAY);
                }

                GridSqlSelect map;

                int paramsCnt = F.isEmpty(params) ? 0 : params.length;

                Integer keysParamIdx = !F.isEmpty(failedKeys) ? paramsCnt + 1 : null;

                if (stmt instanceof GridSqlUpdate)
                    map = GridSqlStatementSplitter.mapQueryForUpdate((GridSqlUpdate) stmt, keysParamIdx);
                else
                    map = GridSqlStatementSplitter.mapQueryForDelete((GridSqlDelete) stmt, keysParamIdx);

                if (keysParamIdx != null) {
                    params = Arrays.copyOf(U.firstNotNull(params, X.EMPTY_OBJECT_ARRAY), paramsCnt + 1);
                    params[paramsCnt] = failedKeys;
                }

                ResultSet rs = indexing.executeSqlQueryWithTimer(cctx.name(), conn, map.getSQL(), F.asList(params), true,
                    timeout, cancel);

                final Iterator<List<?>> rsIter = new IgniteH2Indexing.FieldsIterator(rs);

                Iterable<List<?>> it = new Iterable<List<?>>() {
                    /** {@inheritDoc} */
                    @Override public Iterator<List<?>> iterator() {
                        return rsIter;
                    }
                };

                QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(it);

                if (stmt instanceof GridSqlUpdate)
                    return doUpdate(cctx, (GridSqlUpdate) stmt, cur, 0);
                else
                    return doDelete(cctx, (GridSqlDelete) stmt, cur, 0);
            }
        }
        finally {
            GridH2QueryContext.clearThreadLocal();
        }
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
    static int doSingleUpdate(GridCacheContext cctx, GridTriple<GridSqlElement> singleUpdate,
        GridSqlStatement stmt, Object[] params) throws IgniteCheckedException {
        GridSqlElement target;

        if (stmt instanceof GridSqlUpdate)
            target = ((GridSqlUpdate) stmt).target();
        else if (stmt instanceof GridSqlDelete)
            target = ((GridSqlDelete) stmt).from();
        else
            throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                ErrorCode.PARSE_ERROR_1);

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
     * @param del DELETE statement.
     * @param cursor SELECT results.
     * @param pageSize Page size for streaming, anything <= 0 for single batch operations.
     * @return Results of DELETE (number of items affected AND keys that failed to be updated).
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private IgniteBiTuple<Integer, Object[]> doDelete(GridCacheContext cctx, GridSqlDelete del,
                                                      QueryCursorImpl<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridSqlTable tbl = gridTableForElement(del.from());

        // With DELETE, we have only two columns - key and value.
        int res = 0;

        // Switch to cache specified in query.
        cctx = cctx.shared().cacheContext(CU.cacheId(tbl.schema()));

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
                    GridTuple3<Integer, Object[], SQLException> batchRes = processBatch(cctx, rows);

                    res += batchRes.get1();

                    failedKeys.addAll(F.asList(batchRes.get2()));

                    if (batchRes.get3() != null) {
                        if (resEx == null)
                            resEx = batchRes.get3();
                        else
                            resEx.setNextException(batchRes.get3());
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

        return new IgniteBiTuple<>(res, failedKeys.toArray());
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
    private IgniteBiTuple<Integer, Object[]> doUpdate(GridCacheContext cctx, GridSqlUpdate update,
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

        int res = 0;

        // Switch to cache specified in query.
        cctx = cctx.shared().cacheContext(CU.cacheId(tbl.schema()));

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
                    GridTuple3<Integer, Object[], SQLException> batchRes = processBatch(cctx, rows);

                    res += batchRes.get1();

                    failedKeys.addAll(F.asList(batchRes.get2()));

                    if (batchRes.get3() != null) {
                        if (resEx == null)
                            resEx = batchRes.get3();
                        else
                            resEx.setNextException(batchRes.get3());
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

        return new IgniteBiTuple<>(res, failedKeys.toArray());
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private static GridTuple3<Object[], SQLException, Integer> splitErrors(Map<Object, EntryProcessorResult<Boolean>> res) {
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

        return new GridTuple3<>(errKeys.toArray(), firstSqlEx, errors);
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
    private int doMerge(GridCacheContext cctx, GridSqlMerge gridStmt, QueryCursorImpl<List<?>> cursor, int pageSize)
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

            X.ensureX(prop != null, "Property '" + cols[i].columnName() + "' not found.");

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

        // Switch to cache specified in query.
        cctx = cctx.shared().cacheContext(CU.cacheId(tbl.schema()));

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
                    resCnt += pageSize;

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
    private int doInsert(GridCacheContext cctx, GridSqlInsert ins, QueryCursorImpl<List<?>> cursor, int pageSize)
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

            X.ensureX(prop != null, "Property '" + cols[i].columnName() + "' not found.");

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

        // Switch to cache specified in query.
        cctx = cctx.shared().cacheContext(CU.cacheId(tbl.schema()));

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
                        GridTuple3<Integer, Object[], SQLException> batchRes = processBatch(cctx, rows);

                        resCnt += batchRes.get1();

                        duplicateKeys.addAll(F.asList(batchRes.get2()));

                        if (batchRes.get3() != null) {
                            if (resEx == null)
                                resEx = batchRes.get3();
                            else
                                resEx.setNextException(batchRes.get3());
                        }

                        rows.clear();
                    }
                }

                if (!rows.isEmpty()) {
                    GridTuple3<Integer, Object[], SQLException> batchRes = processBatch(cctx, rows);

                    resCnt += batchRes.get1();

                    duplicateKeys.addAll(F.asList(batchRes.get2()));

                    if (batchRes.get3() != null) {
                        if (resEx == null)
                            resEx = batchRes.get3();
                        else
                            resEx.setNextException(batchRes.get3());
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
    private static GridTuple3<Integer, Object[], SQLException> processBatch(GridCacheContext cctx,
                                                                            Map<Object, EntryProcessor<Object, Object, Boolean>> rows) throws IgniteCheckedException {

        Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows);

        if (res.isEmpty())
            return new GridTuple3<>(rows.size(), null, null);

        GridTuple3<Object[], SQLException, Integer> splitRes = splitErrors(res);

        int keysCnt = F.isEmpty(splitRes.get1()) ? 0 : splitRes.get1().length;

        return new GridTuple3<>(rows.size() - keysCnt - splitRes.get3(), splitRes.get1(), splitRes.get2());
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
     * @param cctx Cache context.
     * @param statement Statement.
     * @param cursor Source data cursor.
     * @param pageSize Page size for streaming.
     * @return Pair [update results cursor; concurrently modified keys]Rnj y
     * @throws IgniteCheckedException if failed.
     */
    IgniteBiTuple<QueryCursorImpl<List<?>>, Object[]> updateFromCursor(GridCacheContext<?, ?> cctx,
        GridSqlStatement statement, QueryCursorImpl<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        IgniteBiTuple<Integer, Object[]> updateRes = null;

        if (statement instanceof GridSqlDelete)
            updateRes = doDelete(cctx, (GridSqlDelete) statement, cursor, pageSize);

        if (statement instanceof GridSqlUpdate)
            updateRes = doUpdate(cctx, (GridSqlUpdate) statement, cursor, pageSize);

        if (statement instanceof GridSqlMerge)
            updateRes = new IgniteBiTuple<>(doMerge(cctx, (GridSqlMerge) statement, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

        if (statement instanceof GridSqlInsert)
            updateRes = new IgniteBiTuple<>(doInsert(cctx, (GridSqlInsert) statement, cursor,
                pageSize), X.EMPTY_OBJECT_ARRAY);

        if (updateRes != null)
            return new IgniteBiTuple<>(QueryCursorImpl.forUpdateResult(updateRes.get1() - updateRes.get2().length),
                updateRes.get2());
        return null;
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
    private static Object getElementValue(GridSqlElement element, Object[] params) throws IgniteCheckedException {
        if (element == null)
            return null;

        if (element instanceof GridSqlConst)
            return ((GridSqlConst)element).value().getObject();
        else if (element instanceof GridSqlParameter)
            return params[((GridSqlParameter)element).index()];
        else
            throw new IgniteCheckedException("Unexpected SQL expression type [cls=" + element.getClass().getName() + ']');
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

        GridSqlStatementSplitter.collectAllGridTablesInTarget(target, tbls);

        if (tbls.size() != 1)
            throw createSqlException("Failed to determine target table", ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        return tbls.iterator().next();
    }

    /**
     * Check that UPDATE statement affects no key columns.
     *
     * @param statement Statement.
     */
    static void verifyUpdateColumns(GridSqlStatement statement) throws IgniteCheckedException {
        if (statement == null || !(statement instanceof GridSqlUpdate))
            return;

        GridSqlUpdate update = (GridSqlUpdate) statement;

        GridSqlElement updTarget = update.target();

        Set<GridSqlTable> tbls = new HashSet<>();

        GridSqlStatementSplitter.collectAllGridTablesInTarget(updTarget, tbls);

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
}
