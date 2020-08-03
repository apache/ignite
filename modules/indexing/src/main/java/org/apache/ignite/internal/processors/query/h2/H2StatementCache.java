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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Statement cache. LRU eviction policy is used. Not thread-safe.
 */
public final class H2StatementCache {
    /** Last usage. */
    private volatile long lastUsage;

    /** */
    private final LinkedHashMap<H2CachedStatementKey, PreparedStatement> lruStmtCache;

    /**
     * @param size Maximum number of statements this cache can store.
     */
    H2StatementCache(int size) {
        lruStmtCache = new LinkedHashMap<H2CachedStatementKey, PreparedStatement>(size, .75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<H2CachedStatementKey, PreparedStatement> eldest) {
                if (size() <= size)
                    return false;

                U.closeQuiet(eldest.getValue());

                return true;
            }
        };
    }

    /**
     * Caches a statement.
     *
     * @param key Key associated with statement.
     * @param stmt Statement which will be cached.
     */
    void put(H2CachedStatementKey key, @NotNull PreparedStatement stmt) {
        lastUsage = System.nanoTime();

        lruStmtCache.put(key, stmt);
    }

    /**
     * Retrieves cached statement.
     *
     * @param key Key for a statement.
     * @return Statement associated with a key.
     */
    @Nullable PreparedStatement get(H2CachedStatementKey key) {
        lastUsage = System.nanoTime();

        return lruStmtCache.get(key);
    }

    /**
     * Checks if the current cache has not been used for at least {@code nanos} nanoseconds.
     *
     * @param nanos Interval in nanoseconds.
     * @return {@code true} if the current cache has not been used for the specified period.
     */
    boolean inactiveFor(long nanos) {
        return System.nanoTime() - lastUsage >= nanos;
    }

    /**
     * Remove statement for given schema and SQL.
     *
     * @param schemaName Schema name.
     * @param sql SQL statement.
     */
    void remove(String schemaName, String sql, byte qryFlags) {
        lastUsage = System.nanoTime();

        lruStmtCache.remove(new H2CachedStatementKey(schemaName, sql, qryFlags));
    }

    /**
     * @return Cache size.
     */
    int size() {
        return lruStmtCache.size();
    }

    /** */
    public static byte queryFlags(QueryDescriptor qryDesc) {
        assert qryDesc != null;

        return queryFlags(qryDesc.distributedJoins(), qryDesc.enforceJoinOrder());
    }

    /** */
    public static byte queryFlags(boolean distributedJoins, boolean enforceJoinOrder) {
        return (byte)((distributedJoins ? 1 : 0) + (enforceJoinOrder ? 2 : 0));
    }
}
