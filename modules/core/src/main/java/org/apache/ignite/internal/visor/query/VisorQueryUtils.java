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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Contains utility methods for Visor query tasks and jobs.
 */
public class VisorQueryUtils {
    /** How long to store future with query in node local map: 5 minutes. */
    public static final Integer RMV_DELAY = 5 * 60 * 1000;

    /** Prefix for node local key for SQL queries. */
    public static final String SQL_QRY_NAME = "VISOR_SQL_QUERY";

    /** Prefix for node local key for SCAN queries. */
    public static final String SCAN_QRY_NAME = "VISOR_SCAN_QUERY";

    /** Columns for SCAN queries. */
    public static final List<VisorQueryField> SCAN_COL_NAMES = Arrays.asList(
        new VisorQueryField(null, null, "Key Class", ""), new VisorQueryField(null, null, "Key", ""),
        new VisorQueryField(null, null, "Value Class", ""), new VisorQueryField(null, null, "Value", "")
    );

    /**
     * @param o - Object.
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
     * @param cur Query future to fetch rows from.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchScanQueryRows(VisorQueryCursor<Cache.Entry<Object, Object>> cur, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        while (cur.hasNext() && cnt < pageSize) {
            Cache.Entry<Object, Object> next = cur.next();

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
     * @param cur Query cursor to fetch rows from.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchSqlQueryRows(VisorQueryCursor<List<?>> cur, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        while (cur.hasNext() && cnt < pageSize) {
            List<?> next = cur.next();

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
     * @param qryId Unique query result id.
     */
    public static void scheduleResultSetHolderRemoval(final String qryId, final IgniteEx ignite) {
        ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
            @Override public void onTimeout() {
                ConcurrentMap<String, VisorQueryCursor> storage = ignite.cluster().nodeLocalMap();

                VisorQueryCursor cur = storage.get(qryId);

                if (cur != null) {
                    // If cursor was accessed since last scheduling, set access flag to false and reschedule.
                    if (cur.accessed()) {
                        cur.accessed(false);

                        scheduleResultSetHolderRemoval(qryId, ignite);
                    }
                    else {
                        // Remove stored cursor otherwise.
                        storage.remove(qryId);

                        cur.close();
                    }
                }
            }
        });
    }
}
