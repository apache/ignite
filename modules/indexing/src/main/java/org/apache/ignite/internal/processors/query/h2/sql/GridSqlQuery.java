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

import java.util.*;

/**
 * Select query.
 */
public abstract class GridSqlQuery implements Cloneable {
    /** */
    protected boolean distinct;

    /** */
    protected List<GridSqlElement> allExprs;

    /** */
    protected List<GridSqlElement> select = new ArrayList<>();

    /** */
    protected Map<GridSqlElement,GridSqlSortColumn> sort = new LinkedHashMap<>();

    /** */
    protected GridSqlElement offset;

    /** */
    protected GridSqlElement limit;

    /**
     * @return Offset.
     */
    public GridSqlElement offset() {
        return offset;
    }

    /**
     * @param offset Offset.
     */
    public void offset(GridSqlElement offset) {
        this.offset = offset;
    }

    /**
     * @param limit Limit.
     */
    public void limit(GridSqlElement limit) {
        this.limit = limit;
    }

    /**
     * @return Limit.
     */
    public GridSqlElement limit() {
        return limit;
    }

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /**
     * @param distinct New distinct.
     */
    public void distinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * @return Generate sql.
     */
    public abstract String getSQL();

    /**
     * @param expression Expression.
     */
    public void addExpression(GridSqlElement expression) {
        if (allExprs == null)
            allExprs = new ArrayList<>();

        allExprs.add(expression);
    }

    /**
     * @return All expressions in select, group by, order by.
     */
    public List<GridSqlElement> allExpressions() {
        return allExprs;
    }

    /**
     * @return Expressions.
     */
    public List<GridSqlElement> select() {
        return select;
    }

    /**
     * Clears select list.
     */
    public void clearSelect() {
        select = new ArrayList<>();
    }

    /**
     * @param expression Expression.
     */
    public void addSelectExpression(GridSqlElement expression) {
        if (expression == null)
            throw new NullPointerException();

        select.add(expression);
    }

    /**
     * @return Sort.
     */
    public Map<GridSqlElement,GridSqlSortColumn> sort() {
        return sort;
    }

    /**
     *
     */
    public void clearSort() {
        sort = new LinkedHashMap<>();
    }

    /**
     * @param expression Expression.
     * @param sortType The sort type bit mask (SortOrder.DESCENDING, SortOrder.NULLS_FIRST, SortOrder.NULLS_LAST).
     */
    public void addSort(GridSqlElement expression, GridSqlSortColumn sortType) {
        sort.put(expression, sortType);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlQuery clone() {
        try {
            GridSqlQuery res = (GridSqlQuery)super.clone();

            res.select = new ArrayList<>(select);
            res.sort = new LinkedHashMap<>(sort);
            res.allExprs = null;

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e); // Never thrown.
        }
    }
}
