/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    /**
     * @return Number of children.
     */
    public int size() {
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