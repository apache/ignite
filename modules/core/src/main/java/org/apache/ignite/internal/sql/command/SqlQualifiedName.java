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
 * SQL qualified name.
 */
public class SqlQualifiedName {
    /** Schema name. */
    private String schemaName;

    /** Object name. */
    private String name;

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
    public SqlQualifiedName schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Object name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name Object name.
     * @return This instance.
     */
    public SqlQualifiedName name(String name) {
        this.name = name;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQualifiedName.class, this);
    }
}
