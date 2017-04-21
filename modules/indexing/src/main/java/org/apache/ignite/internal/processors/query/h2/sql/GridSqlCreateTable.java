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
import org.h2.command.Parser;

/**
 * CREATE TABLE statement.
 */
public class GridSqlCreateTable extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** New schema name. */
    private String newSchemaName;

    /** New cache name. */
    private String newCacheName;

    /** Table name. */
    private String tblName;

    /** Cache name upon which new cache configuration for this table must be based. */
    private String tplCacheName;

    /** Columns. */
    private LinkedHashMap<String, GridSqlColumn> cols;

    /** Affinity column name. */
    private String affColName;

    /** Key class name. */
    private String keyCls;

    /** Value class name. */
    private String valCls;

    public String affinityColumnName() {
        return affColName;
    }

    public void affinityColumnName(String affColName) {
        this.affColName = affColName;
    }

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

    public String newCacheName() {
        return newCacheName;
    }

    public void newCacheName(String newCacheName) {
        this.newCacheName = newCacheName;
    }

    public String newSchemaName() {
        return newSchemaName;
    }

    public void newSchemaName(String newSchemaName) {
        this.newSchemaName = newSchemaName;
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

    public String keyClass() {
        return keyCls;
    }

    public void keyClass(String keyCls) {
        this.keyCls = keyCls;
    }

    public String valueClass() {
        return valCls;
    }

    public void valueClass(String valCls) {
        this.valCls = valCls;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "CREATE TABLE " + Parser.quoteIdentifier(schemaName);
    }
}
