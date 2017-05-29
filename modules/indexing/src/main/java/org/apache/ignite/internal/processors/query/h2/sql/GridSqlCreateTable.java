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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * CREATE TABLE statement.
 */
public class GridSqlCreateTable extends GridSqlStatement {
    /**
     * Schema name upon which this statement has been issued - <b>not</b> the name of the schema where this new table
     * will be created. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Cache name upon which new cache configuration for this table must be based. */
    private String tplCacheName;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Columns. */
    private LinkedHashMap<String, GridSqlColumn> cols;

    /** Primary key columns. */
    private LinkedHashSet<String> pkCols;

    /** Extra WITH-params. */
    private List<String> params;

    /**
     * @return Cache name upon which new cache configuration for this table must be based.
     */
    public String templateCacheName() {
        return tplCacheName;
    }

    /**
     * @param tplCacheName Cache name upon which new cache configuration for this table must be based.
     */
    public void templateCacheName(String tplCacheName) {
        this.tplCacheName = tplCacheName;
    }

    /**
     * @return Columns.
     */
    public LinkedHashMap<String, GridSqlColumn> columns() {
        return cols;
    }

    /**
     * @param cols Columns.
     */
    public void columns(LinkedHashMap<String, GridSqlColumn> cols) {
        this.cols = cols;
    }

    /**
     * @return Primary key columns.
     */
    public LinkedHashSet<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * @param pkCols Primary key columns.
     */
    public void primaryKeyColumns(LinkedHashSet<String> pkCols) {
        this.pkCols = pkCols;
    }

    /**
     * @return Schema name upon which this statement has been issued.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name upon which this statement has been issued.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return Quietly ignore this command if table already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly ignore this command if table already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Extra WITH-params.
     */
    public List<String> params() {
        return params;
    }

    /**
     * @param params Extra WITH-params.
     */
    public void params(List<String> params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return null;
    }
}
