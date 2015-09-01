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

import java.util.Iterator;
import java.util.List;

/**
 * Abstract SQL element.
 */
public abstract class GridSqlElement implements Iterable<GridSqlElement> {
    /** */
    protected List<GridSqlElement> children;

    /** */
    private GridSqlType resultType;

    /**
     * @param children Initial child list.
     */
    protected GridSqlElement(List<GridSqlElement> children) {
        assert children != null;

        this.children = children;
    }

    /**
     * @return Optional expression result type (if this is an expression and result type is known).
     */
    public GridSqlType resultType() {
        return resultType;
    }

    /**
     * @param type Optional expression result type (if this is an expression and result type is known).
     * @return {@code this}.
     */
    public GridSqlElement resultType(GridSqlType type) {
        resultType = type;

        return this;
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
    public <E extends GridSqlElement> E child() {
        return child(0);
    }

    /**
     * @param idx Index.
     * @return Child.
     */
    @SuppressWarnings("unchecked")
    public <E extends GridSqlElement> E child(int idx) {
        return (E)children.get(idx);
    }

    /**
     * @param idx Index.
     * @param child New child.
     */
    public void child(int idx, GridSqlElement child) {
        if (child == null)
            throw new NullPointerException();

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return getSQL();
    }
}