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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DROP INDEX command.
 */
public class SqlDropIndexCommand extends SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Index name. */
    private String idxName;

    /** IF EXISTS flag. */
    private boolean ifExists;

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
    public SqlDropIndexCommand schemaName(String schemaName) {
        this.schemaName = schemaName;

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
    public SqlDropIndexCommand indexName(String idxName) {
        this.idxName = idxName;

        return this;
    }

    /**
     * @return IF EXISTS flag.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /**
     * @param ifExists IF EXISTS flag.
     * @return This instance.
     */
    public SqlDropIndexCommand ifExists(boolean ifExists) {
        this.ifExists = ifExists;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlDropIndexCommand.class, this);
    }
}
