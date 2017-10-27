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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * CREATE INDEX command.
 */
public class SqlCreateIndexCommand extends SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Index name. */
    private String idxName;

    /** IF NOT EXISTS flag. */
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlIndexColumn> cols;

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return This instance.
     */
    public SqlCreateIndexCommand schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     * @return This instance.
     */
    public SqlCreateIndexCommand tableName(String tblName) {
        this.tblName = tblName;

        return this;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @param idxName Index name.
     * @return This instance.
     */
    public SqlCreateIndexCommand indexName(String idxName) {
        this.idxName = idxName;

        return this;
    }

    /**
     * @return IF NOT EXISTS flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists IF NOT EXISTS flag.
     * @return This instance.
     */
    public SqlCreateIndexCommand ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;

        return this;
    }

    /**
     * @return Columns.
     */
    public Collection<SqlIndexColumn> columns() {
        return cols != null ? cols : Collections.<SqlIndexColumn>emptySet();
    }

    /**
     * @param col Column.
     * @return This instance.
     */
    public SqlCreateIndexCommand addColumn(SqlIndexColumn col) {
        if (cols == null)
            cols = new LinkedList<>();

        cols.add(col);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateIndexCommand.class, this);
    }
}
