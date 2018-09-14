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

package org.apache.ignite.internal.processors.query.schema.operation;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema alter table drop column operation.
 */
public class SchemaAlterTableDropColumnOperation extends SchemaAbstractAlterTableOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Target table name. */
    private final String tblName;

    /** Columns to drop. */
    private final List<String> cols;

    /** Ignore operation if target table doesn't exist. */
    private final boolean ifTblExists;

    /** Ignore operation if column does not exist. */
    private final boolean ifExists;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param schemaName Schema name.
     * @param tblName Target table name.
     * @param cols Columns to drop.
     * @param ifTblExists Ignore operation if target table doesn't exist.
     * @param ifExists Ignore operation if column does not exist.
     */
    public SchemaAlterTableDropColumnOperation(UUID opId, String cacheName, String schemaName, String tblName,
        List<String> cols, boolean ifTblExists, boolean ifExists) {
        super(opId, cacheName, schemaName);

        this.tblName = tblName;
        this.cols = cols;
        this.ifTblExists = ifTblExists;
        this.ifExists = ifExists;
    }

    /**
     * @return Ignore operation if table doesn't exist.
     */
    public boolean ifTableExists() {
        return ifTblExists;
    }

    /**
     * @return Columns to drop.
     */
    public List<String> columns() {
        return cols;
    }

    /**
     * @return Quietly abort this command if column does not exist (honored only in single column case).
     */
    public boolean ifExists() {
        return ifExists;
    }

    /**
     * @return Target table name.
     */
    public String tableName() {
        return tblName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaAlterTableDropColumnOperation.class, this, "parent", super.toString());
    }
}
