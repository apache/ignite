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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Statement cache.
 */
class H2StatementCache extends LinkedHashMap<H2CachedStatementKey, PreparedStatement> {
    /** */
    private int size;

    /** Last usage. */
    private volatile long lastUsage;

    /**
     * @param size Size.
     */
    H2StatementCache(int size) {
        super(size, (float)0.75, true);

        this.size = size;
    }

    /** {@inheritDoc} */
    @Override protected boolean removeEldestEntry(Map.Entry<H2CachedStatementKey, PreparedStatement> eldest) {
        boolean rmv = size() > size;

        if (rmv) {
            PreparedStatement stmt = eldest.getValue();

            U.closeQuiet(stmt);
        }

        return rmv;
    }

    /**
     * Get statement for given schema and SQL.
     * @param schemaName Schema name.
     * @param sql SQL statement.
     * @return Cached {@link PreparedStatement}, or {@code null} if none found.
     */
    @Nullable public PreparedStatement get(String schemaName, String sql) {
        return get(new H2CachedStatementKey(schemaName, sql));
    }

    /**
     * The timestamp of the last usage of the cache.
     *
     * @return last usage timestamp
     */
    public long lastUsage() {
        return lastUsage;
    }

    /**
     * Updates the {@link #lastUsage} timestamp by current time.
     */
    public void updateLastUsage() {
        lastUsage = U.currentTimeMillis();
    }

    /**
     * Remove statement for given schema and SQL.
     * @param schemaName Schema name.
     * @param sql SQL statement.
     * @return Cached {@link PreparedStatement}, or {@code null} if none found.
     */
    @Nullable public PreparedStatement remove(String schemaName, String sql) {
        return remove(new H2CachedStatementKey(schemaName, sql));
    }
}
