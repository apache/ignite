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
public abstract class GridSqlElement implements Cloneable {
    /** */
    protected List<GridSqlElement> children = new ArrayList<>();

    /** {@inheritDoc} */
    public abstract String getSQL();

    /**
     * @return Children.
     */
    public List<GridSqlElement> children() {
        return children;
    }

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
    @Override protected GridSqlElement clone() {
        try {
            GridSqlElement res = (GridSqlElement)super.clone();

            res.children = new ArrayList<>(children);

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
