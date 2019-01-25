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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.jetbrains.annotations.Nullable;

import java.util.TreeSet;

/**
 * Utility methods for splitter.
 */
public class SplitterUtils {
    /**
     * Check whether AST element has aggregates.
     *
     * @param el Expression part in SELECT clause.
     * @return {@code true} If expression contains aggregates.
     */
    public static boolean hasAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction)
            return true;

        // If in SELECT clause we have a subquery expression with aggregate,
        // we should not split it. Run the whole subquery on MAP stage.
        if (el instanceof GridSqlSubquery)
            return false;

        for (int i = 0; i < el.size(); i++) {
            if (hasAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param paramIdxs Parameter indexes.
     */
    public static void findParamsQuery(GridSqlQuery qry, Object[] params, TreeSet<Integer> paramIdxs) {
        if (qry instanceof GridSqlSelect)
            findParamsSelect((GridSqlSelect)qry, params, paramIdxs);
        else {
            GridSqlUnion union = (GridSqlUnion)qry;

            findParamsQuery(union.left(), params, paramIdxs);
            findParamsQuery(union.right(), params, paramIdxs);

            findParams(qry.limit(), params, paramIdxs);
            findParams(qry.offset(), params, paramIdxs);
        }
    }

    /**
     * @param select Select.
     * @param params Parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParamsSelect(GridSqlSelect select, Object[] params, TreeSet<Integer> paramIdxs) {
        if (params.length == 0)
            return;

        for (GridSqlAst el : select.columns(false))
            findParams(el, params, paramIdxs);

        findParams(select.from(), params, paramIdxs);
        findParams(select.where(), params, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams(select.limit(), params, paramIdxs);
        findParams(select.offset(), params, paramIdxs);
    }

    /**
     * @param el Element.
     * @param params Parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParams(@Nullable GridSqlAst el, Object[] params, TreeSet<Integer> paramIdxs) {
        if (el == null)
            return;

        if (el instanceof GridSqlParameter) {
            // H2 Supports queries like "select ?5" but first 4 non-existing parameters are need to be set to any value.
            // Here we will set them to NULL.
            final int idx = ((GridSqlParameter)el).index();

            if (params.length <= idx)
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + idx + " parameter.");

            paramIdxs.add(idx);
        }
        else if (el instanceof GridSqlSubquery)
            findParamsQuery(((GridSqlSubquery)el).subquery(), params, paramIdxs);
        else {
            for (int i = 0; i < el.size(); i++)
                findParams(el.child(i), params, paramIdxs);
        }
    }

    /**
     * Private constructor.
     */
    private SplitterUtils() {
        // No-op.
    }
}
