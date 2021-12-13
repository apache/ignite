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

/**
 * Abstract table ddl command.
 */
public class AbstractTableDdlCommand implements DdlCommand {
    /** Table name. */
    private String tblName;

    /** Quietly ignore this command if table is not exists. */
    protected boolean ifTableExists;

    /** Schema name where this new table will be created. */
    private String commanCurrentSchema;

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

    /**
     * Quietly ignore if table exists flag.
     */
    public boolean ifTableExists() {
        return ifTableExists;
    }

    /**
     * Set quietly ignore flag.
     *
     * @param ifTableNotExists Flag.
     */
    public void ifTableExists(boolean ifTableNotExists) {
        this.ifTableExists = ifTableNotExists;
    }
}
