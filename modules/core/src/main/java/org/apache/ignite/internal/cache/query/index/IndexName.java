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

package org.apache.ignite.internal.cache.query.index;

import org.jetbrains.annotations.Nullable;

/**
 * Represents list of names that fully describes index domain (schema, cache, table, index).
 */
public class IndexName {
    /** Schema name of {@code null} if index is not related to SQL schema. */
    private final @Nullable String schemaName;

    /** Schema name of {@code null} if index is not related to SQL table. */
    private final @Nullable String tableName;

    /** Cache name. */
    private final String cacheName;

    /** Index name. */
    private final String idxName;

    /** */
    public IndexName(String cacheName, @Nullable String schemaName, @Nullable String tableName, String idxName) {
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.idxName = idxName;
    }

    /**
     * @return Full index name.
     */
    public String fullName() {
        StringBuilder bld = new StringBuilder();

        if (schemaName != null)
            bld.append(schemaName).append(".");

        if (tableName != null)
            bld.append(tableName).append(".");

        return bld.append(idxName).toString();
    }

    /**
     * @return Index name.
     */
    public String idxName() {
        return idxName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return fullName();
    }
}
