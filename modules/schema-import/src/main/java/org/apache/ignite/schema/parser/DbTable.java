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

package org.apache.ignite.schema.parser;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Database table.
 */
public class DbTable {
    /** Schema name. */
    private final String schema;

    /** Table name. */
    private final String tbl;

    /** Columns. */
    private final Collection<DbColumn> cols;

    /** Columns in ascending order. */
    private final Set<String> ascCols;

    /** Columns in descending order. */
    private final Set<String> descCols;

    /** Indexes. */
    private final Map<String, Map<String, Boolean>> idxs;

    /**
     * Default columns.
     *
     * @param schema Schema name.
     * @param tbl Table name.
     * @param cols Columns.
     * @param ascCols Columns in ascending order.
     * @param descCols Columns in descending order.
     * @param idxs Indexes;
     */
    public DbTable(String schema, String tbl, Collection<DbColumn> cols, Set<String> ascCols, Set<String> descCols,
        Map<String, Map<String, Boolean>> idxs) {
        this.schema = schema;
        this.tbl = tbl;
        this.cols = cols;
        this.ascCols = ascCols;
        this.descCols = descCols;
        this.idxs = idxs;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String table() {
        return tbl;
    }

    /**
     * @return Columns.
     */
    public Collection<DbColumn> columns() {
        return cols;
    }

    /**
     * @return Fields in ascending order
     */
    public Set<String> ascendingColumns() {
        return ascCols;
    }

    /**
     * @return Fields in descending order
     */
    public Set<String> descendingColumns() {
        return descCols;
    }

    /**
     * @return Indexes.
     */
    public Map<String, Map<String, Boolean>> indexes() {
        return idxs;
    }
}