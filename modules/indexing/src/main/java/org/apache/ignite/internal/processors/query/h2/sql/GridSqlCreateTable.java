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
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.Parser;
import org.h2.table.Column;
import org.h2.value.DataType;
import org.h2.value.Value;

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
    private LinkedHashMap<String, GridSqlColumn> pkCols;

    public String templateCacheName() {
        return tplCacheName;
    }

    public void templateCacheName(String tplCacheName) {
        this.tplCacheName = tplCacheName;
    }

    public LinkedHashMap<String, GridSqlColumn> columns() {
        return cols;
    }

    public void columns(LinkedHashMap<String, GridSqlColumn> cols) {
        this.cols = cols;
    }

    public LinkedHashMap<String, GridSqlColumn> primaryKeyColumns() {
        return pkCols;
    }

    public void primaryKeyColumns(LinkedHashMap<String, GridSqlColumn> pkCols) {
        this.pkCols = pkCols;
    }

    public String schemaName() {
        return schemaName;
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String tableName() {
        return tblName;
    }

    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "CREATE TABLE " + Parser.quoteIdentifier(schemaName);
    }

    /**
     * Convert this statement to query entity and do Ignite specific sanity checks on the way.
     * @return Query entity mimicking this SQL statement.
     */
    public QueryEntity toQueryEntity() {
        QueryEntity res = new QueryEntity();

        res.setTableName(tableName());

        if (columns().containsKey(IgniteH2Indexing.KEY_FIELD_NAME) ||
            columns().containsKey(IgniteH2Indexing.VAL_FIELD_NAME))
            throw new IgniteSQLException("Direct specification of _KEY and _VAL columns is forbidden",
                IgniteQueryErrorCode.PARSING);

        for (Map.Entry<String, GridSqlColumn> e : columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), DataType.getTypeClassName(col.getType()), null);
        }

        if (F.isEmpty(pkCols))
            throw new IgniteSQLException("No PRIMARY KEY columns specified");

        int valColsNum = res.getFields().size() - pkCols.size();

        if (valColsNum == 0)
            throw new IgniteSQLException("No cache value related columns found");

        res.setKeyType(tableName() + "Key");

        res.setValueType(tableName());

        res.setKeyFields(pkCols.keySet());

        return res;
    }
}
