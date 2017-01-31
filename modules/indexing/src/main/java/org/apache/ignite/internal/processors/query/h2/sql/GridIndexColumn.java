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

import java.util.Collections;
import org.h2.command.Parser;

/**
 * One of the columns that make up an index.
 */
public class GridIndexColumn extends GridSqlElement {
    /** Column name. */
    private String name;

    /** Ordering. */
    private boolean ascending;

    /** */
    public GridIndexColumn() {
        super(Collections.<GridSqlElement>emptyList());
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return Parser.quoteIdentifier(name) + (ascending ? " ASC" : " DESC");
    }

    /**
     * @return Index column name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name Index column name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Whether index order is ascending for this column.
     */
    public boolean ascending() {
        return ascending;
    }

    /**
     * @param ascending Whether index order is ascending for this column.
     */
    public void ascending(boolean ascending) {
        this.ascending = ascending;
    }
}
