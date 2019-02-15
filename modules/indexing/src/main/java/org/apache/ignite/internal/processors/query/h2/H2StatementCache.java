/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Statement cache. LRU eviction policy is used. Not thread-safe.
 */
final class H2StatementCache {
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
        lruStmtCache.put(key, stmt);
    }

    /**
     * Retrieves cached statement.
     *
     * @param key Key for a statement.
     * @return Statement associated with a key.
     */
    @Nullable PreparedStatement get(H2CachedStatementKey key) {
        return lruStmtCache.get(key);
    }

    /**
     * The timestamp of the last usage of the cache.
     *
     * @return last usage timestamp
     */
    long lastUsage() {
        return lastUsage;
    }

    /**
     * Updates the {@link #lastUsage} timestamp by current time.
     */
    void updateLastUsage() {
        lastUsage = U.currentTimeMillis();
    }

    /**
     * Remove statement for given schema and SQL.
     *
     * @param schemaName Schema name.
     * @param sql SQL statement.
     */
    void remove(String schemaName, String sql) {
        lruStmtCache.remove(new H2CachedStatementKey(schemaName, sql));
    }

    /**
     * @return Cache size.
     */
    int size() {
        return lruStmtCache.size();
    }
}
