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

package org.apache.ignite.cache.store.jdbc.dialect;

import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Represents a dialect of SQL implemented by a particular RDBMS.
 */
public class BasicJdbcDialect implements JdbcDialect {
    /** Default max query parameters count. */
    protected static final int DFLT_MAX_PARAMS_CNT = 2000;

    /** Max query parameters count. */
    protected int maxParamsCnt = DFLT_MAX_PARAMS_CNT;

    /**
     * Concatenates elements using provided separator.
     *
     * @param elems Concatenated elements.
     * @param f closure used for transform element.
     * @param start Start string.
     * @param sep Separator.
     * @param end End string.
     * @return Concatenated string.
     */
    protected static <T> String mkString(Iterable<T> elems, C1<T, String> f, String start, String sep, String end) {
        SB sb = new SB(start);

        boolean first = true;

        for (T elem : elems) {
            if (!first)
                sb.a(sep);

            sb.a(f.apply(elem));

            first = false;
        }

        return sb.a(end).toString();
    }

    /**
     * Concatenates elements using provided separator.
     *
     * @param strs Concatenated string.
     * @param start Start string.
     * @param sep Delimiter.
     * @param end End string.
     * @return Concatenated string.
     */
    protected static String mkString(Iterable<String> strs, String start, String sep, String end) {
        return mkString(strs, new C1<String, String>() {
            @Override public String apply(String s) {
                return s;
            }
        }, start, sep, end);
    }

    /**
     * Concatenates strings using provided separator.
     *
     * @param strs Concatenated string.
     * @param sep Separator.
     * @return Concatenated string.
     */
    protected static String mkString(Iterable<String> strs, String sep) {
        return mkString(strs, new C1<String, String>() {
            @Override public String apply(String s) {
                return s;
            }
        }, "", sep, "");
    }

    /**
     * Concatenates elements using provided delimiter.
     *
     * @param str Repeated string.
     * @param cnt Repeat count.
     * @param start Start string.
     * @param sep Separator.
     * @param end End string.
     */
    protected static String repeat(String str, int cnt, String start, String sep, String end) {
        SB sb = new SB(str.length() * cnt + sep.length() * (cnt - 1) + start.length() + end.length());

        sb.a(start);

        for (int i = 0; i < cnt; i++) {
            if (i > 0)
                sb.a(sep);

            sb.a(str);
        }

        return sb.a(end).toString();
    }

    /**
     * Construct where part of query.
     *
     * @param keyCols Database key columns.
     * @param keyCnt Key count.
     */
    private static String where(Collection<String> keyCols, int keyCnt) {
        SB sb = new SB();

        if (keyCols.size() == 1) {
            String keyCol = keyCols.iterator().next();

            if (keyCnt == 1)
                sb.a(keyCol + "=?");
            else
                sb.a(repeat("?", keyCnt, keyCol + " IN (", ",", ")"));
        }
        else {
            String keyParams = mkString(keyCols, new C1<String, String>() {
                @Override public String apply(String s) {
                    return s + "=?";
                }
            }, "(", " AND ", ")");

            sb.a(repeat(keyParams, keyCnt, "", " OR ", ""));
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public String loadCacheQuery(String schema, String tblName, Iterable<String> uniqCols) {
        return String.format("SELECT %s FROM %s.%s", mkString(uniqCols, ","), schema, tblName);
    }

    /** {@inheritDoc} */
    @Override public String loadQuery(String schema, String tblName, Collection<String> keyCols, Iterable<String> cols,
        int keyCnt) {
        assert !keyCols.isEmpty();

        String params = where(keyCols, keyCnt);

        return String.format("SELECT %s FROM %s.%s WHERE %s", mkString(cols, ","), schema, tblName, params);
    }

    /** {@inheritDoc} */
    @Override public String insertQuery(String schema, String tblName, Collection<String> keyCols,
        Collection<String> valCols) {
        Collection<String> cols = F.concat(false, keyCols, valCols);

        return String.format("INSERT INTO %s.%s(%s) VALUES(%s)", schema, tblName, mkString(cols, ","),
            repeat("?", cols.size(), "", ",", ""));
    }

    /** {@inheritDoc} */
    @Override public String updateQuery(String schema, String tblName, Collection<String> keyCols,
        Iterable<String> valCols) {
        String params = mkString(valCols, new C1<String, String>() {
            @Override public String apply(String s) {
                return s + "=?";
            }
        }, "", ",", "");

        return String.format("UPDATE %s.%s SET %s WHERE %s", schema, tblName, params, where(keyCols, 1));
    }

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String schema, String tblName, Collection<String> keyCols,
        Collection<String> uniqCols) {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String removeQuery(String schema, String tblName, Iterable<String> keyCols) {
        String whereParams = mkString(keyCols, new C1<String, String>() {
            @Override public String apply(String s) {
                return s + "=?";
            }
        }, "", " AND ", "");

        return String.format("DELETE FROM %s.%s WHERE %s", schema, tblName, whereParams);
    }

    /** {@inheritDoc} */
    @Override public int getMaxParamsCnt() {
        return maxParamsCnt;
    }

    /**
     * Set max query parameters count.
     *
     * @param maxParamsCnt Max query parameters count.
     */
    public void setMaxParamsCnt(int maxParamsCnt) {
        this.maxParamsCnt = maxParamsCnt;
    }
}
