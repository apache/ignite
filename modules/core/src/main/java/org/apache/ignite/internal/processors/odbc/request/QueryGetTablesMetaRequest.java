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
package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query get tables meta request.
 */
public class QueryGetTablesMetaRequest extends GridOdbcRequest {
    /** Catalog search pattern. */
    private String catalog;

    /** Schema search pattern. */
    private String schema;

    /** Table search pattern. */
    private String table;

    /** Table type search pattern. */
    private String tableType;

    /**
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param tableType Table type search pattern.
     */
    public QueryGetTablesMetaRequest(String catalog, String schema, String table, String tableType) {
        super(GET_TABLES_META);

        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.tableType = tableType;
    }

    /**
     * @param catalog Catalog search pattern.
     */
    public void catalog(String catalog) {
        this.catalog = catalog;
    }

    /**
     * @return catalog search pattern.
     */
    public String catalog() {
        return catalog;
    }

    /**
     * @param schema Schema search pattern.
     */
    public void schema(String schema) {
        this.schema = schema;
    }

    /**
     * @return Schema search pattern.
     */
    public String schema() {
        return schema;
    }

    /**
     * @param table Schema search pattern.
     */
    public void table(String table) {
        this.table = table;
    }

    /**
     * @return Table search pattern.
     */
    public String table() {
        return table;
    }

    /**
     * @param tableType Table type search pattern.
     */
    public void tableType(String tableType) {
        this.tableType = tableType;
    }

    /**
     * @return Table type search pattern.
     */
    public String tableType() {
        return tableType;
    }
}