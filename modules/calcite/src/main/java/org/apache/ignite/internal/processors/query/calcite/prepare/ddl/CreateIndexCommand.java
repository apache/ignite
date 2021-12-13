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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.List;
import org.apache.ignite.internal.util.Pair;

/**
 * CREATE INDEX statement.
 */
public class CreateIndexCommand implements DdlCommand {
    /** Table name. */
    private String tblName;

    /** Quietly ignore this command if table is not exists. */
    protected boolean ifTableNotExists;

    /** Schema name where this new table will be created. */
    private String commanCurrentSchema;

    /** Idx name. */
    private String indexName;

    /** Quietly ignore this command if index already exists. */
    private boolean ifIdxNotExists;

    /** Colunms covered with ordering. */
    List<Pair<String, Boolean>> cols;

    /** Return idx name. */
    public String indexName() {
        return indexName;
    }

    /** Set idx name. */
    public void indexName(String indexName) {
        this.indexName = indexName;
    }

    public List<Pair<String, Boolean>> columns() {
        return cols;
    }

    public void columns(List<Pair<String, Boolean>> cols) {
        this.cols = cols;
    }

    /**
     * Quietly ignore this command if index already exists.
     *
     * @return Quietly ignore flag.
     */
    public boolean ifIndexNotExists() {
        return ifIdxNotExists;
    }

    /**
     * Quietly ignore this command if index already exists.
     *
     * @param ifIdxNotExists Exists flag.
     */
    public void ifIndexNotExists(boolean ifIdxNotExists) {
        this.ifIdxNotExists = ifIdxNotExists;
    }

    public String tableName() {
        return tblName;
    }

    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    public String schemaName() {
        return commanCurrentSchema;
    }

    public void schemaName(String schemaName) {
        this.commanCurrentSchema = schemaName;
    }
}
