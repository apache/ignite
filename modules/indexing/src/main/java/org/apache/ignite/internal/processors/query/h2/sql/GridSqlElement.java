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
 * Abstract SQL element.
 */
public abstract class GridSqlElement implements Cloneable, Iterable<GridSqlElement> {
    /** */
    protected List<GridSqlElement> children = new ArrayList<>();

    /** */
    private GridSqlType expressionResultType;

    /**
     * @return Optional expression result type (if this is an expression and result type is known).
     */
    public GridSqlType expressionResultType() {
        return expressionResultType;
    }

    /**
     * @param type Optional expression result type (if this is an expression and result type is known).
     */
    public void expressionResultType(GridSqlType type) {
        expressionResultType = type;
    }

    /**
     * Get the SQL expression.
     *
     * @return the SQL expression.
     */
    public abstract String getSQL();

    /**
     * @param expr Expr.
     * @return {@code this}.
     */
    public GridSqlElement addChild(GridSqlElement expr) {
        if (expr == null)
            throw new NullPointerException();

        children.add(expr);

        return this;
    }

    /**
     * @return First child.
     */
    public GridSqlElement child() {
        return children.get(0);
    }

    /**
     * @param idx Index.
     * @return Child.
     */
    public GridSqlElement child(int idx) {
        return children.get(idx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlElement clone() {
        try {
            GridSqlElement res = (GridSqlElement)super.clone();

            res.children = new ArrayList<>(children);

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param idx Index.
     * @param child New child.
     */
    public void child(int idx, GridSqlElement child) {
        children.set(idx, child);
    }

    /**
     * @return Number of children.
     */
    public int size() {
        return children.size();
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridSqlElement> iterator() {
        return children.iterator();
    }
}
