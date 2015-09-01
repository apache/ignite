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

/**
 * Sort order for ORDER BY clause.
 */
public class GridSqlSortColumn {
    /** */
    private final int col;

    /** */
    private final boolean asc;

    /** */
    private final boolean nullsFirst;

    /** */
    private final boolean nullsLast;

    /**
     * @param col Column index.
     * @param asc Ascending.
     * @param nullsFirst Nulls go first.
     * @param nullsLast Nulls go last.
     */
    public GridSqlSortColumn(int col, boolean asc, boolean nullsFirst, boolean nullsLast) {
        this.col = col;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.nullsLast = nullsLast;
    }

    /**
     * @return Column index.
     */
    public int column() {
        return col;
    }

    /**
     * @return {@code true} For ASC order.
     */
    public boolean asc() {
        return asc;
    }

    /**
     * @return {@code true} If {@code null}s must go first.
     */
    public boolean nullsFirst() {
        return nullsFirst;
    }

    /**
     * @return {@code true} If {@code null}s must go last.
     */
    public boolean nullsLast() {
        return nullsLast;
    }
}