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

import java.util.Map;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.h2.command.Parser;

/**
 * CREATE INDEX statement.
 */
public class GridSqlCreateIndex extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Attempt to create the index only if it does not exist. */
    private boolean ifNotExists;

    /** Index to create. */
    private QueryIndex idx;

    /**
     * @return Schema name for new index.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name for new index.
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
     * @return whether attempt to create the index should be made only if it does not exist.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists whether attempt to create the index should be made only if it does not exist.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Index to create.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @param idx Index to create.
     */
    public void index(QueryIndex idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder sb = new StringBuilder("CREATE ")
            .append(idx.getIndexType() == QueryIndexType.GEOSPATIAL ? "SPATIAL " : "")
            .append("INDEX ").append(ifNotExists ? "IF NOT EXISTS " : "")
            .append(Parser.quoteIdentifier(schemaName)).append('.')
            .append(Parser.quoteIdentifier(idx.getName())).append(" ON ")
            .append(Parser.quoteIdentifier(tblName)).append(" (");

        boolean first = true;

        for (Map.Entry<String, Boolean> e : idx.getFields().entrySet()) {
            if (first)
                first = false;
            else
                sb.append(", ");

            sb.append(Parser.quoteIdentifier(e.getKey())).append(e.getValue() ? " ASC" : " DESC");
        }

        sb.append(')');

        return sb.toString();
    }
}
