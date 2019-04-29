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
 * Query parameter.
 */
public class GridSqlParameter extends GridSqlElement {
    /** Index. */
    private int idx;

    /**
     * @param idx Index.
     */
    public GridSqlParameter(int idx) {
        super(Collections.<GridSqlAst>emptyList());

        this.idx = idx;
    }

    /** {@inheritDoc}  */
    @Override public String getSQL() {
        return "?" + (idx + 1);
    }

    /**
     * @return Index.
     */
    public int index() {
        return idx;
    }

    /**
     * @param idx New index.
     */
    public void index(int idx) {
        this.idx = idx;
    }
}