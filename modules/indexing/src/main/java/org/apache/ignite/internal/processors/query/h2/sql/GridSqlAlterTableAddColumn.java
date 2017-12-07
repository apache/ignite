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

/**
 * ALTER TABLE ADD COLUMN command data holder.
 */
public class GridSqlAlterTableAddColumn extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** Target table name. */
    private String tblName;

    /** Columns to add. */
    private GridSqlColumn[] cols;

    /** Quietly abort this command if column exists (honored only in single column case). */
    private boolean ifNotExists;

    /** Quietly abort this command if target table does not exist. */
    private boolean ifTblExists;

    /**
     * @return Columns to add.
     */
    public GridSqlColumn[] columns() {
        return cols;
    }

    /**
     * @param cols Columns to add.
     */
    public void columns(GridSqlColumn[] cols) {
        this.cols = cols;
    }

    /**
     * @return Quietly abort this command if column exists (honored only in single column case).
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly abort this command if column exists (honored only in single column case).
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Quietly abort this command if target table does not exist.
     */
    public boolean ifTableExists() {
        return ifTblExists;
    }

    /**
     * @param ifTblExists Quietly abort this command if target table does not exist.
     */
    public void ifTableExists(boolean ifTblExists) {
        this.ifTblExists = ifTblExists;
    }

    /**
     * @return Target table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Target table name.
     */
    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        throw new UnsupportedOperationException();
    }
}
