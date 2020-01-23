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

import java.util.List;

/**
 * Base class for all SQL AST nodes.
 */
public abstract class GridSqlElement implements GridSqlAst {
    /** */
    private final List<GridSqlAst> children;

    /** */
    private GridSqlType resultType;

    /**
     * @param children Initial child list.
     */
    protected GridSqlElement(List<GridSqlAst> children) {
        assert children != null;

        this.children = children;
    }

    /** {@inheritDoc} */
    @Override public GridSqlType resultType() {
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
     * @param expr Expr.
     * @return {@code this}.
     */
    public GridSqlElement addChild(GridSqlAst expr) {
        if (expr == null)
            throw new NullPointerException();

        children.add(expr);

        return this;
    }

    /** {@inheritDoc} */
    @Override public <E extends GridSqlAst> E child() {
        return child(0);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <E extends GridSqlAst> E child(int idx) {
        return (E)children.get(idx);
    }

    /** {@inheritDoc} */
    @Override public <E extends GridSqlAst> void child(int idx, E child) {
        if (child == null)
            throw new NullPointerException();

        children.set(idx, child);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return children.size();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getSQL();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) &&
            children.equals(((GridSqlElement)o).children));
    }
}
