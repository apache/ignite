/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Collections;

/**
 * Placeholder.
 */
public class GridSqlPlaceholder extends GridSqlElement {
    /** */
    public static final GridSqlPlaceholder EMPTY = new GridSqlPlaceholder("");

    /** */
    private final String sql;

    /**
     * @param sql SQL.
     */
    public GridSqlPlaceholder(String sql) {
        super(Collections.<GridSqlAst>emptyList());

        this.sql = sql;
    }

    /** {@inheritDoc}  */
    @Override public String getSQL() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override public GridSqlElement resultType(GridSqlType type) {
        throw new IllegalStateException();
    }
}