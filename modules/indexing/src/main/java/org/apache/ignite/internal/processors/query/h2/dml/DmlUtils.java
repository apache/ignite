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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.lang.reflect.Array;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.h2.util.DateTimeUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * DML utility methods.
 */
public class DmlUtils {
    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param desc Row descriptor.
     * @param expCls Expected value class.
     * @param type Expected column type to convert to.
     * @return Converted object.
     */
    @SuppressWarnings({"ConstantConditions", "SuspiciousSystemArraycopy"})
    public static Object convert(Object val, GridH2RowDescriptor desc, Class<?> expCls,
        int type, String columnName) {
        if (val == null)
            return null;

        Class<?> currCls = val.getClass();

        try {
            // H2 thinks that java.util.Date is always a Timestamp, while binary marshaller expects
            // precise Date instance. Let's satisfy it.
            if (val instanceof Date && currCls != Date.class && expCls == Date.class)
                return new Date(((Date) val).getTime());

            // User-given UUID is always serialized by H2 to byte array, so we have to deserialize manually
            if (type == Value.UUID && currCls == byte[].class) {
                return U.unmarshal(desc.context().marshaller(), (byte[])val,
                    U.resolveClassLoader(desc.context().gridConfig()));
            }

            if (val instanceof Timestamp && LocalDateTimeUtils.LOCAL_DATE_TIME == expCls)
                return LocalDateTimeUtils.valueToLocalDateTime(ValueTimestamp.get((Timestamp)val));

            if (val instanceof Date && LocalDateTimeUtils.LOCAL_DATE == expCls) {
                return LocalDateTimeUtils.valueToLocalDate(ValueDate.fromDateValue(
                    DateTimeUtils.dateValueFromDate(((Date)val).getTime())));
            }

            if (val instanceof Time && LocalDateTimeUtils.LOCAL_TIME == expCls)
                return LocalDateTimeUtils.valueToLocalTime(ValueTime.get((Time)val));

            // We have to convert arrays of reference types manually -
            // see https://issues.apache.org/jira/browse/IGNITE-4327
            // Still, we only can convert from Object[] to something more precise.
            if (type == Value.ARRAY && currCls != expCls) {
                if (currCls != Object[].class) {
                    throw new IgniteCheckedException("Unexpected array type - only conversion from Object[] " +
                        "is assumed");
                }

                // Why would otherwise type be Value.ARRAY?
                assert expCls.isArray();

                Object[] curr = (Object[])val;

                Object newArr = Array.newInstance(expCls.getComponentType(), curr.length);

                System.arraycopy(curr, 0, newArr, 0, curr.length);

                return newArr;
            }

            Object res = H2Utils.convert(val, desc.indexing(), type);

            // We can get a Timestamp instead of Date when converting a String to Date
            // without query - let's handle this
            if (res instanceof Date && res.getClass() != Date.class && expCls == Date.class)
                return new Date(((Date)res).getTime());

            return res;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Value conversion failed [column=" + columnName + ", from=" + currCls.getName() + ", to=" +
                expCls.getName() + ']', IgniteQueryErrorCode.CONVERSION_FAILED, e);
        }
    }

    /**
     * Check whether query is batched.
     *
     * @param qry Query.
     * @return {@code True} if batched.
     */
    public static boolean isBatched(SqlFieldsQuery qry) {
        return (qry instanceof SqlFieldsQueryEx) && ((SqlFieldsQueryEx)qry).isBatched();
    }

    /**
     * @param plan Update plan.
     * @param cursor Cursor over select results.
     * @param pageSize Page size.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    public static UpdateResult processSelectResult(UpdatePlan plan, Iterable<List<?>> cursor,
        int pageSize) throws IgniteCheckedException {
        switch (plan.mode()) {
            case MERGE:
                return new UpdateResult(doMerge(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

            case INSERT:
                return new UpdateResult(dmlDoInsert(plan, cursor, pageSize), X.EMPTY_OBJECT_ARRAY);

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
     * Execute INSERT statement plan.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    @SuppressWarnings({"unchecked"})
    private static long dmlDoInsert(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize) throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        // If we have just one item to put, just do so
        if (plan.rowCount() == 1) {
            IgniteBiTuple t = plan.processRow(cursor.iterator().next());

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw new TransactionDuplicateKeyException("Duplicate key during INSERT [key=" + t.getKey() + ']');
        }
        else {
            // Keys that failed to INSERT due to duplication.
            DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

            for (List<?> row : cursor) {
                final IgniteBiTuple keyValPair = plan.processRow(row);

                sender.add(keyValPair.getKey(), new DmlStatementsProcessor.InsertEntryProcessor(keyValPair.getValue()), 0);
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
     * Perform UPDATE operation on top of results of SELECT.
     * @param cursor SELECT results.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Pair [cursor corresponding to results of UPDATE (contains number of items affected); keys whose values
     *     had been modified concurrently (arguments for a re-run)].
     */
    private static UpdateResult doUpdate(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

        for (List<?> row : cursor) {
            T3<Object, Object, Object> row0 = plan.processRowForUpdate(row);

            Object key = row0.get1();
            Object oldVal = row0.get2();
            Object newVal = row0.get3();

            sender.add(key, new DmlStatementsProcessor.ModifyingEntryProcessor(
                oldVal,
                new DmlStatementsProcessor.EntryValueUpdater(newVal)),
                0
            );
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

        return new UpdateResult(sender.updateCount(), sender.failedKeys().toArray(),
            cursor instanceof QueryCursorImpl ? ((QueryCursorImpl) cursor).partitionResult() : null);
    }

    /**
     * Execute MERGE statement plan.
     * @param cursor Cursor to take inserted data from.
     * @param pageSize Batch size to stream data from {@code cursor}, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private static long doMerge(UpdatePlan plan, Iterable<List<?>> cursor, int pageSize) throws IgniteCheckedException {
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
     * Perform DELETE operation on top of results of SELECT.
     *
     * @param cctx Cache context.
     * @param cursor SELECT results.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Results of DELETE (number of items affected AND keys that failed to be updated).
     */
    private static UpdateResult doDelete(GridCacheContext cctx, Iterable<List<?>> cursor, int pageSize)
        throws IgniteCheckedException {
        DmlBatchSender sender = new DmlBatchSender(cctx, pageSize, 1);

        for (List<?> row : cursor) {
            if (row.size() != 2)
                continue;

            Object key = row.get(0);

            ClusterNode node = sender.primaryNodeByKey(key);

            IgniteInClosure<MutableEntry<Object, Object>> rmvC =
                DmlStatementsProcessor.getRemoveClosure(node, key);

            sender.add(
                key,
                new DmlStatementsProcessor.ModifyingEntryProcessor(row.get(1), rmvC),
                0
            );
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

        return new UpdateResult(sender.updateCount(), sender.failedKeys().toArray(),
            cursor instanceof QueryCursorImpl ? ((QueryCursorImpl) cursor).partitionResult() : null);
    }

    /**
     * Performs the planned update.
     * @param plan Update plan.
     * @param rows Rows to update.
     * @param pageSize Page size.
     * @return {@link List} of update results.
     * @throws IgniteCheckedException If failed.
     */
    public static List<UpdateResult> processSelectResultBatched(UpdatePlan plan, List<List<List<?>>> rows, int pageSize)
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
     * Execute INSERT statement plan.
     *
     * @param plan Plan to execute.
     * @param cursor Cursor to take inserted data from. I.e. list of batch arguments for each query.
     * @param pageSize Batch size for streaming, anything <= 0 for single page operations.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    private static List<UpdateResult> doInsertBatched(UpdatePlan plan, List<List<List<?>>> cursor, int pageSize)
        throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        DmlBatchSender snd = new DmlBatchSender(cctx, pageSize, cursor.size());

        int rowNum = 0;

        SQLException resEx = null;

        for (List<List<?>> qryRow : cursor) {
            for (List<?> row : qryRow) {
                try {
                    final IgniteBiTuple keyValPair = plan.processRow(row);

                    snd.add(keyValPair.getKey(), new DmlStatementsProcessor.InsertEntryProcessor(keyValPair.getValue()), rowNum);
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

            res.add(new UpdateResult(cnt, X.EMPTY_OBJECT_ARRAY));
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
    public static SQLException chainException(SQLException main, SQLException add) {
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
     * Makes current operation context as keepBinary.
     *
     * @param cctx Cache context.
     * @return Old operation context.
     */
    public static CacheOperationContext setKeepBinaryContext(GridCacheContext<?, ?> cctx) {
        CacheOperationContext opCtx = cctx.operationContextPerCall();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        if (cctx.binaryMarshaller()) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null, false, false, true);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        return opCtx;
    }

    /**
     * Restore previous binary context.
     *
     * @param cctx Cache context.
     * @param oldOpCtx Old operation context.
     */
    public static void restoreKeepBinaryContext(GridCacheContext<?, ?> cctx, CacheOperationContext oldOpCtx) {
        cctx.operationContextPerCall(oldOpCtx);
    }

    /**
     * Private constructor.
     */
    private DmlUtils() {
        // No-op.
    }
}
