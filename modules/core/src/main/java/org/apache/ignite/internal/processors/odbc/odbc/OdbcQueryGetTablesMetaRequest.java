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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query get tables meta request.
 */
public class OdbcQueryGetTablesMetaRequest extends OdbcRequest {
    /** Catalog search pattern. */
    private final String catalog;

    /** Schema search pattern. */
    private final String schema;

    /** Table search pattern. */
    private final String table;

    /** Table type search pattern. */
    private final String tableType;

    /**
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param tableType Table type search pattern.
     */
    public OdbcQueryGetTablesMetaRequest(String catalog, String schema, String table, String tableType) {
        super(META_TBLS);

        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.tableType = tableType;
    }

    /**
     * @return catalog search pattern.
     */
    public String catalog() {
        return catalog;
    }

    /**
     * @return Schema search pattern.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table search pattern.
     */
    public String table() {
        return table;
    }

    /**
     * @return Table type search pattern.
     */
    public String tableType() {
        return tableType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetTablesMetaRequest.class, this);
    }
}