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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.tostring.*;

import java.util.*;

/**
 * Database table metadata.
 */
public class CacheQueryTableMetadata {
    /** Schema name in database. */
    private String schema;

    /** Table name in database. */
    private String tbl;

    /** Key columns. */
    @GridToStringInclude
    private Collection<CacheQueryTableColumnMetadata> keyCols;

    /** Value columns . */
    @GridToStringInclude
    private Collection<CacheQueryTableColumnMetadata> valCols;

    /**
     * Default constructor.
     */
    public CacheQueryTableMetadata() {
        keyCols = new ArrayList<>();
        valCols = new ArrayList<>();
    }

    /**
     * Copy constructor.
     *
     * @param src Source table metadata.
     */
    public CacheQueryTableMetadata(CacheQueryTableMetadata src) {
        schema = src.getSchema();
        tbl = src.getTableName();

        keyCols = new ArrayList<>(src.getKeyColumns());
        valCols = new ArrayList<>(src.getValueColumns());
    }

    /**
     * Gets database schema name.
     *
     * @return Schema name.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets database schema name.
     *
     * @param schema Schema name.
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Gets table name in database.
     *
     * @return Table name in database.
     */
    public String getTableName() {
        return tbl;
    }

    /**
     * Table name in database.
     *
     * @param tbl Table name in database.
     */
    public void setTableName(String tbl) {
        this.tbl = tbl;
    }

    /**
     * Gets key columns.
     *
     * @return Key columns.
     */
    public Collection<CacheQueryTableColumnMetadata> getKeyColumns() {
        return keyCols;
    }

    /**
     * Sets key columns.
     *
     * @param keyCols New key columns.
     */
    public void setKeyColumns(Collection<CacheQueryTableColumnMetadata> keyCols) {
        this.keyCols = keyCols;
    }

    /**
     * Gets value columns.
     *
     * @return Value columns.
     */
    public Collection<CacheQueryTableColumnMetadata> getValueColumns() {
        return valCols;
    }

    /**
     * Sets value columns.
     *
     * @param valCols New value columns.
     */
    public void setValueColumns(Collection<CacheQueryTableColumnMetadata> valCols) {
        this.valCols = valCols;
    }
}
