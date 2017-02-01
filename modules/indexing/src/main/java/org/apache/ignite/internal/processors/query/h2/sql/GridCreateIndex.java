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
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.h2.command.Parser;

/**
 * CREATE INDEX statement.
 */
public class GridCreateIndex extends GridSqlStatement {
    /**
     * Index name.
     */
    private String name;

    /**
     * Schema name.
     */
    private String schemaName;

    /**
     * Table name.
     */
    private String tblName;

    /**
     * Index cols.
     */
    private GridIndexColumn[] cols;

    /**
     * Whether SPATIAL index creation has been requested.
     */
    private boolean spatial;

    /** */
    private boolean ifNotExists;

    /**
     * @return Index name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name Index name.
     */
    public void name(String name) {
        this.name = name;
    }

    public String schemaName() {
        return schemaName;
    }

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
     * @return Index cols.
     */
    public GridIndexColumn[] columns() {
        return cols;
    }

    /**
     * @param cols Index cols.
     */
    public void columns(GridIndexColumn[] cols) {
        this.cols = cols;
    }

    /**
     * @return Whether SPATIAL index creation has been requested.
     */
    public boolean spatial() {
        return spatial;
    }

    /**
     * @param spatial Whether SPATIAL index creation has been requested.
     */
    public void spatial(boolean spatial) {
        this.spatial = spatial;
    }

    /** */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /** */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder sb = new StringBuilder("CREATE " + (spatial ? "SPATIAL " : "") + "INDEX " +
            (ifNotExists ? "IF NOT EXISTS " : "") + Parser.quoteIdentifier(schemaName) + '.' +
            Parser.quoteIdentifier(name) + " ON " + Parser.quoteIdentifier(tblName) + " (");

        boolean first = true;

        for (GridIndexColumn c : cols) {
            if (first)
                first = false;
            else
                sb.append(", ");

            sb.append(c.getSQL());
        }

        return sb.toString();
    }

    public QueryIndex toQueryIndex() {
        QueryIndex res = new QueryIndex(name);
        res.setIndexType(spatial ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>(cols.length);

        for (GridIndexColumn col : cols)
            flds.put(col.name(), col.ascending());

        res.setFields(flds);
        return res;
    }
}
