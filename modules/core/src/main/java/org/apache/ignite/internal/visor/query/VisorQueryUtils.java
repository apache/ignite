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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.internal.util.IgniteUtils;

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
    public static final Collection<VisorQueryField> SCAN_COL_NAMES = Arrays.asList(
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

            for (int i = 0; i < sz; i++) {
                Object o = next.get(i);

                if (o == null)
                    row[i] = null;
                else if (isKnownType(o))
                    row[i] = o;
                else
                    row[i] = o.getClass().isArray() ? "binary" : o.toString();
            }

            rows.add(row);

            cnt++;
        }

        return rows;
    }
}