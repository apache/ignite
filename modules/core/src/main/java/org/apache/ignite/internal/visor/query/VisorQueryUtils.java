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

package org.apache.ignite.internal.visor.query;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;

/**
 * Contains utility methods for Visor query tasks and jobs.
 */
public class VisorQueryUtils {
    /** How long to store future with query in node local map: 5 minutes. */
    public static final Integer RMV_DELAY = 5 * 60 * 1000;

    /** Message for query result expired error. */
    private static final String SQL_QRY_RESULTS_EXPIRED_ERR = "SQL query results are expired.";

    /** Message for scan result expired error. */
    private static final String SCAN_QRY_RESULTS_EXPIRED_ERR = "Scan query results are expired.";

    /** Columns for SCAN queries. */
    public static final List<VisorQueryField> SCAN_COL_NAMES = Arrays.asList(
        new VisorQueryField(null, null, "Key Class", ""), new VisorQueryField(null, null, "Key", ""),
        new VisorQueryField(null, null, "Value Class", ""), new VisorQueryField(null, null, "Value", "")
    );

    /**
     * @param o Source object.
     * @return String representation of object class.
     */
    private static String typeOf(Object o) {
        if (o != null) {
            Class<?> clazz = o.getClass();

            return clazz.isArray() ? IgniteUtils.compact(clazz.getComponentType().getName()) + "[]"
                : IgniteUtils.compact(o.getClass().getName());
        }
        else
            return "n/a";
    }

    /**
     * @param o Object.
     * @return String representation of value.
     */
    private static String valueOf(Object o) {
        if (o == null)
            return "null";

        if (o instanceof byte[])
            return "size=" + ((byte[])o).length;

        if (o instanceof Byte[])
            return "size=" + ((Byte[])o).length;

        if (o instanceof Object[])
            return "size=" + ((Object[])o).length + ", values=[" + mkString((Object[])o, 120) + "]";

        if (o instanceof BinaryObject)
            return binaryToString((BinaryObject)o);

        return o.toString();
    }

    /**
     * @param arr Object array.
     * @param maxSz Maximum string size.
     * @return Fixed size string.
     */
    private static String mkString(Object[] arr, int maxSz) {
        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        boolean first = true;

        for (Object v : arr) {
            if (first)
                first = false;
            else
                sb.append(sep);

            sb.append(v);

            if (sb.length() > maxSz)
                break;
        }

        if (sb.length() >= maxSz) {
            String end = "...";

            sb.setLength(maxSz - end.length());

            sb.append(end);
        }

        return sb.toString();
    }

    /**
     * Fetch rows from SCAN query future.
     *
     * @param itr Result set iterator.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchScanQueryRows(Iterator itr, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        Iterator<Cache.Entry<Object, Object>> scanItr = (Iterator<Cache.Entry<Object, Object>>)itr;

        while (scanItr.hasNext() && cnt < pageSize) {
            Cache.Entry<Object, Object> next = scanItr.next();

            Object k = next.getKey();
            Object v = next.getValue();

            rows.add(new Object[] {typeOf(k), valueOf(k), typeOf(v), valueOf(v)});

            cnt++;
        }

        return rows;
    }

    /**
     * Checks is given object is one of known types.
     *
     * @param obj Object instance to check.
     * @return {@code true} if it is one of known types.
     */
    private static boolean isKnownType(Object obj) {
        return obj instanceof String ||
            obj instanceof Boolean ||
            obj instanceof Byte ||
            obj instanceof Integer ||
            obj instanceof Long ||
            obj instanceof Short ||
            obj instanceof Date ||
            obj instanceof Double ||
            obj instanceof Float ||
            obj instanceof BigDecimal ||
            obj instanceof URL;
    }

    /**
     * Convert Binary object to string.
     *
     * @param obj Binary object.
     * @return String representation of Binary object.
     */
    public static String binaryToString(BinaryObject obj) {
        int hash = obj.hashCode();

        if (obj instanceof BinaryObjectEx) {
            BinaryObjectEx objEx = (BinaryObjectEx)obj;

            BinaryType meta;

            try {
                meta = ((BinaryObjectEx)obj).rawType();
            }
            catch (BinaryObjectException ignore) {
                meta = null;
            }

            if (meta != null) {
                if (meta.isEnum()) {
                    try {
                        return obj.deserialize().toString();
                    }
                    catch (BinaryObjectException ignore) {
                        // NO-op.
                    }
                }

                SB buf = new SB(meta.typeName());

                if (meta.fieldNames() != null) {
                    buf.a(" [hash=").a(hash);

                    for (String name : meta.fieldNames()) {
                        Object val = objEx.field(name);

                        buf.a(", ").a(name).a('=').a(val);
                    }

                    buf.a(']');

                    return buf.toString();
                }
            }
        }

        return S.toString(obj.getClass().getSimpleName(),
            "hash", hash, false,
            "typeId", obj.type().typeId(), true);
    }

    /**
     * Convert object that can be passed to client.
     *
     * @param original Source object.
     * @return Converted value.
     */
    public static Object convertValue(Object original) {
        if (original == null)
            return null;
        else if (isKnownType(original))
            return original;
        else if (original instanceof BinaryObject)
            return binaryToString((BinaryObject)original);
        else
            return original.getClass().isArray() ? "binary" : original.toString();
    }

    /**
     * Collects rows from sql query future, first time creates meta and column names arrays.
     *
     * @param itr Result set iterator.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchSqlQueryRows(Iterator itr, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        Iterator<List<?>> sqlItr = (Iterator<List<?>>)itr;

        while (sqlItr.hasNext() && cnt < pageSize) {
            List<?> next = sqlItr.next();

            int sz = next.size();

            Object[] row = new Object[sz];

            for (int i = 0; i < sz; i++)
                row[i] = convertValue(next.get(i));

            rows.add(row);

            cnt++;
        }

        return rows;
    }

    /**
     * Get holder for query or throw exception if not found.
     *
     * @param ignite IgniteEx instance.
     * @param qryId Query ID to get holder.
     * @return Query holder for specified query ID.
     * @throws IgniteException When holder is not found.
     */
    public static VisorQueryHolder getQueryHolder(final IgniteEx ignite, final String qryId) throws IgniteException {
        ConcurrentMap<String, VisorQueryHolder> storage = ignite.cluster().nodeLocalMap();

        VisorQueryHolder holder = storage.get(qryId);

        if (holder == null)
            throw new IgniteException(VisorQueryHolder.isSqlQuery(qryId)
                ? SQL_QRY_RESULTS_EXPIRED_ERR
                : SCAN_QRY_RESULTS_EXPIRED_ERR);

        return holder;
    }

    /**
     * Remove query holder from local storage for query with specified ID and cancel query if it is in progress.
     *
     * @param ignite IgniteEx instance.
     * @param qryId Query ID to get holder.
     */
    public static void removeQueryHolder(final IgniteEx ignite, final String qryId) {
        ConcurrentMap<String, VisorQueryHolder> storage = ignite.cluster().nodeLocalMap();
        VisorQueryHolder holder = storage.remove(qryId);

        if (holder != null)
            holder.close();
    }

    /**
     * Fetch rows from query cursor.
     *
     * @param itr Result set iterator.
     * @param qryId Query ID.
     * @param pageSize Page size.
     */
    public static List<Object[]> fetchQueryRows(Iterator itr, String qryId, int pageSize) {
        return itr.hasNext()
            ? (VisorQueryHolder.isSqlQuery(qryId)
                ? fetchSqlQueryRows(itr, pageSize)
                : fetchScanQueryRows(itr, pageSize))
            : Collections.emptyList();
    }

    /**
     * Schedule start of SQL query execution.
     *
     * @param ignite IgniteEx instance.
     * @param holder Query holder object.
     * @param arg Query task argument with query properties.
     * @param cancel Object to cancel query.
     */
    public static void scheduleQueryStart(
        final IgniteEx ignite,
        final VisorQueryHolder holder,
        final VisorQueryTaskArg arg,
        final GridQueryCancel cancel
    ) {
        ignite.context().closure().runLocalSafe(() -> {
            try {
                SqlFieldsQuery qry = new SqlFieldsQuery(arg.getQueryText());

                qry.setPageSize(arg.getPageSize());
                qry.setLocal(arg.isLocal());
                qry.setDistributedJoins(arg.isDistributedJoins());
                qry.setCollocated(arg.isCollocated());
                qry.setEnforceJoinOrder(arg.isEnforceJoinOrder());
                qry.setReplicatedOnly(arg.isReplicatedOnly());
                qry.setLazy(arg.getLazy());

                String cacheName = arg.getCacheName();

                if (!F.isEmpty(cacheName))
                    qry.setSchema(cacheName);

                long start = U.currentTimeMillis();

                List<FieldsQueryCursor<List<?>>> qryCursors = ignite
                    .context()
                    .query()
                    .querySqlFields(null, qry, null, true, false, cancel);

                // In case of multiple statements leave opened only last cursor.
                for (int i = 0; i < qryCursors.size() - 1; i++)
                    U.closeQuiet(qryCursors.get(i));

                // In case of multiple statements return last cursor as result.
                FieldsQueryCursor<List<?>> cur = F.last(qryCursors);

                try {
                    // Ensure holder was not removed from node local storage from separate thread if user cancel query.
                    VisorQueryHolder actualHolder = getQueryHolder(ignite, holder.getQueryID());

                    List<GridQueryFieldMetadata> meta = ((QueryCursorEx)cur).fieldsMeta();

                    if (meta == null)
                        actualHolder.setError(new SQLException("Fail to execute query. No metadata available."));
                    else {
                        List<VisorQueryField> cols = new ArrayList<>(meta.size());

                        for (GridQueryFieldMetadata col : meta) {
                            cols.add(new VisorQueryField(
                                col.schemaName(),
                                col.typeName(),
                                col.fieldName(),
                                col.fieldTypeName())
                            );
                        }

                        actualHolder.complete(cur, U.currentTimeMillis() - start, cols);

                        scheduleQueryHolderRemoval(ignite, actualHolder.getQueryID());
                    }
                }
                catch (Throwable e) {
                    U.closeQuiet(cur);

                    throw e;
                }
            }
            catch (Throwable e) {
                holder.setError(e);
            }
        }, MANAGEMENT_POOL);
    }

    /**
     * Schedule start of SCAN query execution.
     *
     * @param ignite IgniteEx instance.
     * @param holder Query holder object.
     * @param arg Query task argument with query properties.
     */
    public static void scheduleScanStart(
        final IgniteEx ignite,
        final VisorQueryHolder holder,
        final VisorScanQueryTaskArg arg
    ) {
        ignite.context().closure().runLocalSafe(() -> {
            try {
                IgniteCache<Object, Object> c = ignite.cache(arg.getCacheName());
                String filterText = arg.getFilter();
                IgniteBiPredicate<Object, Object> filter = null;

                if (!F.isEmpty(filterText))
                    filter = new VisorQueryScanRegexFilter(arg.isCaseSensitive(), arg.isRegEx(), filterText);

                QueryCursor<Cache.Entry<Object, Object>> cur;

                long start = U.currentTimeMillis();

                if (arg.isNear())
                    cur = new VisorNearCacheCursor<>(c.localEntries(CachePeekMode.NEAR).iterator());
                else {
                    ScanQuery<Object, Object> qry = new ScanQuery<>(filter);
                    qry.setPageSize(arg.getPageSize());
                    qry.setLocal(arg.isLocal());

                    cur = c.withKeepBinary().query(qry);
                }

                try {
                    // Ensure holder was not removed from node local storage from separate thread if user cancel query.
                    VisorQueryHolder actualHolder = getQueryHolder(ignite, holder.getQueryID());

                    actualHolder.complete(cur, U.currentTimeMillis() - start, SCAN_COL_NAMES);

                    scheduleQueryHolderRemoval(ignite, actualHolder.getQueryID());
                }
                catch (Throwable e) {
                    U.closeQuiet(cur);

                    throw e;
                }
            }
            catch (Throwable e) {
                holder.setError(e);
            }
        }, MANAGEMENT_POOL);
    }

    /**
     * Wrapper for cache iterator to behave like {@link QueryCursor}.
     */
    private static class VisorNearCacheCursor<T> implements QueryCursor<T> {
        /** Wrapped iterator.  */
        private final Iterator<T> it;

        /**
         * Wrapping constructor.
         *
         * @param it Near cache iterator to wrap.
         */
        private VisorNearCacheCursor(Iterator<T> it) {
            this.it = it;
        }

        /** {@inheritDoc} */
        @Override public List<T> getAll() {
            List<T> all = new ArrayList<>();

            while (it.hasNext())
                all.add(it.next());

            return all;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // Nothing to close.
        }

        /** {@inheritDoc} */
        @Override public Iterator<T> iterator() {
            return it;
        }
    }

    /**
     * Schedule clearing of query context by timeout.
     *
     * @param qryId Unique query result id.
     * @param ignite IgniteEx instance.
     */
    public static void scheduleQueryHolderRemoval(final IgniteEx ignite, final String qryId) {
        ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
            @Override public void onTimeout() {
                ConcurrentMap<String, VisorQueryHolder> storage = ignite.cluster().nodeLocalMap();

                VisorQueryHolder holder = storage.get(qryId);

                if (holder != null) {
                    if (holder.isAccessed()) {
                        holder.setAccessed(false);

                        // Holder was accessed, we need to keep it for one more period.
                        scheduleQueryHolderRemoval(ignite, qryId);
                    }
                    else {
                        // Remove stored cursor otherwise.
                        removeQueryHolder(ignite, qryId);
                    }
                }
            }
        });
    }
}
