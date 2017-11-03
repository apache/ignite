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

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Basic implementation of dialect based on JDBC specification.
 */
public class BasicJdbcDialect implements JdbcDialect {
    /** */
    private static final long serialVersionUID = 0L;

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
    @Override public String escape(String ident) {
        return '"' + ident + '"';
    }

    /** {@inheritDoc} */
    @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
        String cols = mkString(keyCols, ",");

        return String.format("SELECT %1$s FROM (SELECT %1$s, ROW_NUMBER() OVER() AS rn FROM (SELECT %1$s FROM %2$s ORDER BY %1$s) AS tbl) AS tbl WHERE mod(rn, ?) = 0",
            cols, fullTblName);
    }

    /** {@inheritDoc} */
    @Override public String loadCacheRangeQuery(String fullTblName,
        Collection<String> keyCols, Iterable<String> uniqCols, boolean appendLowerBound, boolean appendUpperBound) {
        assert appendLowerBound || appendUpperBound;

        SB sb = new SB();

        String[] cols = keyCols.toArray(new String[keyCols.size()]);

        if (appendLowerBound) {
            sb.a("(");

            for (int keyCnt = keyCols.size(); keyCnt > 0; keyCnt--) {
                for (int idx = 0; idx < keyCnt; idx++) {
                    if (idx == keyCnt - 1)
                        sb.a(cols[idx]).a(" > ? ");
                    else
                        sb.a(cols[idx]).a(" = ? AND ");
                }

                if (keyCnt != 1)
                    sb.a("OR ");
            }

            sb.a(")");
        }

        if (appendLowerBound && appendUpperBound)
            sb.a(" AND ");

        if (appendUpperBound) {
            sb.a("(");

            for (int keyCnt = keyCols.size(); keyCnt > 0; keyCnt--) {
                for (int idx = 0, lastIdx = keyCnt - 1; idx < keyCnt; idx++) {
                    sb.a(cols[idx]);

                    // For composite key when not all of the key columns are constrained should use < (strictly less).
                    if (idx == lastIdx)
                        sb.a(keyCnt == keyCols.size() ? " <= ? " : " < ? ");
                    else
                        sb.a(" = ? AND ");
                }

                if (keyCnt != 1)
                    sb.a(" OR ");
            }

            sb.a(")");
        }

        return String.format("SELECT %s FROM %s WHERE %s", mkString(uniqCols, ","), fullTblName, sb.toString());
    }

    /** {@inheritDoc} */
    @Override public String loadCacheQuery(String fullTblName, Iterable<String> uniqCols) {
        return String.format("SELECT %s FROM %s", mkString(uniqCols, ","), fullTblName);
    }

    /** {@inheritDoc} */
    @Override public String loadQuery(String fullTblName, Collection<String> keyCols, Iterable<String> cols,
        int keyCnt) {
        assert !keyCols.isEmpty();

        String params = where(keyCols, keyCnt);

        return String.format("SELECT %s FROM %s WHERE %s", mkString(cols, ","), fullTblName, params);
    }

    /** {@inheritDoc} */
    @Override public String insertQuery(String fullTblName, Collection<String> keyCols,
        Collection<String> valCols) {
        Collection<String> cols = F.concat(false, keyCols, valCols);

        return String.format("INSERT INTO %s(%s) VALUES(%s)", fullTblName, mkString(cols, ","),
            repeat("?", cols.size(), "", ",", ""));
    }

    /** {@inheritDoc} */
    @Override public String updateQuery(String fullTblName, Collection<String> keyCols,
        Iterable<String> valCols) {
        String params = mkString(valCols, new C1<String, String>() {
            @Override public String apply(String s) {
                return s + "=?";
            }
        }, "", ",", "");

        return String.format("UPDATE %s SET %s WHERE %s", fullTblName, params, where(keyCols, 1));
    }

    /** {@inheritDoc} */
    @Override public boolean hasMerge() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols) {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String removeQuery(String fullTblName, Iterable<String> keyCols) {
        String whereParams = mkString(keyCols, new C1<String, String>() {
            @Override public String apply(String s) {
                return s + "=?";
            }
        }, "", " AND ", "");

        return String.format("DELETE FROM %s WHERE %s", fullTblName, whereParams);
    }

    /** {@inheritDoc} */
    @Override public int getMaxParameterCount() {
        return maxParamsCnt;
    }

    /**
     * Set max query parameters count.
     *
     * @param maxParamsCnt Max query parameters count.
     */
    public void setMaxParameterCount(int maxParamsCnt) {
        this.maxParamsCnt = maxParamsCnt;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() {
        return 0;
    }
}
